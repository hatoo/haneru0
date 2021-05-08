use std::ops::Range;
use std::{
    borrow::Cow,
    mem::{size_of, MaybeUninit},
};

use byteorder::{ByteOrder, NativeEndian};
use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use crate::{
    buffer,
    disk::{self, PageId, PAGE_SIZE},
    freelist::FreeList,
};

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
struct OverFlowedHeader {
    next_page_id: PageId,
}

#[derive(FromBytes)]
#[repr(C)]
struct OverFlowedPage<B> {
    header: LayoutVerified<B, OverFlowedHeader>,
    body: B,
}

impl<B: ByteSlice> OverFlowedPage<B> {
    fn new(bytes: B) -> Self {
        let (header, body) = LayoutVerified::new_from_prefix(bytes).unwrap();
        Self { header, body }
    }
}

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
struct Header {
    num_slots: u16,
    free_space_offset: u16,
    free_space: u16,
    // 0 if no freed block
    first_freed_block_offset: u16,
}

// We can't use `zerocopy` for this struct since this may not be aligned.
struct FreedBlock {
    len: u16,
    // 0 if tail
    next_freed_block_offset: u16,
}

struct Overflow {
    len: u64,
    page_id: PageId,
}

impl Overflow {
    fn read(bytes: &[u8]) -> Self {
        Self {
            len: NativeEndian::read_u64(&bytes[..8]),
            page_id: PageId(NativeEndian::read_u64(&bytes[8..16])),
        }
    }

    fn write(&self, bytes: &mut [u8]) {
        NativeEndian::write_u64(&mut bytes[..8], self.len);
        NativeEndian::write_u64(&mut bytes[8..16], self.page_id.0);
    }
}

#[derive(Debug, FromBytes, AsBytes, Clone, Copy, Eq, PartialEq)]
#[repr(C)]
struct Length(u16);

impl Length {
    pub const OVERFLOWED: Length = Length(u16::MAX);
    fn valid(self) -> Option<usize> {
        if self == Self::OVERFLOWED {
            None
        } else {
            Some(self.0 as usize)
        }
    }

    fn actual_len(self) -> usize {
        if self == Self::OVERFLOWED {
            size_of::<Overflow>()
        } else {
            self.0 as usize
        }
    }
}

#[derive(Debug, FromBytes, AsBytes, Clone, Copy)]
#[repr(C)]
struct Pointer {
    offset: u16,
    len: Length,
}

impl Pointer {
    fn range(&self) -> Range<usize> {
        let start = self.offset as usize;
        let end = start + self.len.actual_len();
        start..end
    }
}

type Pointers<B> = LayoutVerified<B, [Pointer]>;

pub struct SlottedOverflow<B> {
    header: LayoutVerified<B, Header>,
    body: B,
}

struct FreedBlockIter<'a, B> {
    body: &'a B,
    prev: u16,
    current: u16,
}

#[derive(Debug, FromBytes, AsBytes, Clone, Copy)]
#[repr(C)]
struct SimplePointer {
    offset: u16,
    len: u16,
}

impl SimplePointer {
    fn range(&self) -> Range<usize> {
        let start = self.offset as usize;
        let end = start + self.len as usize;
        start..end
    }
}

#[derive(Debug)]
struct FreedPointer {
    pointer: SimplePointer,
    prev_freed_block_offset: u16,
    next_freed_block_offset: u16,
}

impl FreedBlock {
    fn read(bytes: &[u8]) -> Self {
        Self {
            len: NativeEndian::read_u16(&bytes[..2]),
            next_freed_block_offset: NativeEndian::read_u16(&bytes[2..4]),
        }
    }

    fn write(&self, bytes: &mut [u8]) {
        NativeEndian::write_u16(&mut bytes[..2], self.len);
        NativeEndian::write_u16(&mut bytes[2..4], self.next_freed_block_offset);
    }

    fn write_next_freed_offset(bytes: &mut [u8], next_freed_offset: u16) {
        NativeEndian::write_u16(&mut bytes[2..4], next_freed_offset);
    }

    fn write_len(bytes: &mut [u8], len: u16) {
        NativeEndian::write_u16(&mut bytes[0..2], len);
    }
}

impl<'a, B: ByteSlice> Iterator for FreedBlockIter<'a, B> {
    type Item = FreedPointer;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == 0 {
            None
        } else {
            let freed_block = FreedBlock::read(&self.body[self.current as usize..]);
            let ret = Some(FreedPointer {
                pointer: SimplePointer {
                    offset: self.current,
                    len: freed_block.len,
                },
                prev_freed_block_offset: self.prev,
                next_freed_block_offset: freed_block.next_freed_block_offset,
            });

            self.prev = self.current;
            self.current = freed_block.next_freed_block_offset;
            ret
        }
    }
}

impl<B> SlottedOverflow<B> {
    const MAX_PAYLOAD_SIZE: usize = 1024;

    fn actual_len(len: usize) -> usize {
        if len > Self::MAX_PAYLOAD_SIZE {
            size_of::<Overflow>()
        } else {
            len
        }
    }
}

impl<B: ByteSlice> SlottedOverflow<B> {
    pub fn new(bytes: B) -> Self {
        let (header, body) =
            LayoutVerified::new_from_prefix(bytes).expect("slotted header must be aligned");
        assert!(body.len() <= PAGE_SIZE);
        Self { header, body }
    }

    pub fn capacity(&self) -> usize {
        self.body.len()
    }

    pub fn num_slots(&self) -> usize {
        self.header.num_slots as usize
    }

    pub fn free_capacity(&self) -> usize {
        self.header.free_space as usize
    }

    fn free_space(&self) -> usize {
        self.header.free_space_offset as usize
            - size_of::<Pointer>() * self.header.num_slots as usize
    }

    fn freed_blocks(&self) -> FreedBlockIter<'_, B> {
        FreedBlockIter {
            body: &self.body,
            prev: 0,
            current: self.header.first_freed_block_offset,
        }
    }

    fn pointers_size(&self) -> usize {
        size_of::<Pointer>() * self.num_slots()
    }

    fn pointers(&self) -> Pointers<&[u8]> {
        Pointers::new_slice(&self.body[..self.pointers_size()]).unwrap()
    }

    async fn fetch_overflowed_page(
        page_id: PageId,
        len: usize,
        free_list: &FreeList,
    ) -> Result<Vec<u8>, buffer::Error> {
        let mut current_page_id = page_id;
        let mut remaining_len = len;
        let mut buf = Vec::with_capacity(len);

        while remaining_len > 0 {
            if remaining_len <= disk::PAGE_SIZE {
                let page = free_list.fetch_page(current_page_id).await?;
                let lock = page.page.read().await;
                buf.extend_from_slice(&lock[..remaining_len]);
                remaining_len = 0;
            } else {
                let page = free_list.fetch_page(current_page_id).await?;
                let lock = page.page.read().await;
                let overflowed_page = OverFlowedPage::new(lock.as_bytes());
                remaining_len -= overflowed_page.body.len();
                current_page_id = overflowed_page.header.next_page_id;
                buf.extend_from_slice(overflowed_page.body.as_bytes());
            }
        }

        Ok(buf)
    }

    pub async fn fetch(
        &self,
        index: usize,
        free_list: &FreeList,
    ) -> Result<Cow<'_, [u8]>, buffer::Error> {
        let pointer = self.pointers()[index];

        if pointer.len.valid().is_some() {
            Ok(Cow::Borrowed(&self.body[pointer.range()]))
        } else {
            let overflow = Overflow::read(&self.body[pointer.range()]);
            Ok(Cow::Owned(
                Self::fetch_overflowed_page(overflow.page_id, overflow.len as usize, free_list)
                    .await?,
            ))
        }
    }
}

impl<B: ByteSliceMut> SlottedOverflow<B> {
    pub fn initialize(&mut self) {
        self.header.num_slots = 0;
        self.header.free_space_offset = self.body.len() as u16;
        self.header.free_space = self.body.len() as u16;
        self.header.first_freed_block_offset = 0;
    }

    fn pointers_mut(&mut self) -> Pointers<&mut [u8]> {
        let pointers_size = self.pointers_size();
        Pointers::new_slice(&mut self.body[..pointers_size]).unwrap()
    }

    fn defrag(&mut self) {
        let body_len = self.body.len();
        let mut free_space_offset = body_len;
        let ptr_offset = self.pointers_size();
        #[allow(clippy::uninit_assumed_init)]
        let mut buf: [u8; PAGE_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
        let (pointers, body) = self.body.split_at_mut(ptr_offset);
        let mut pointers = Pointers::new_slice(pointers).unwrap();
        for pointer in pointers.iter_mut() {
            buf[free_space_offset - pointer.len.actual_len()..free_space_offset].copy_from_slice(
                &body[pointer.offset as usize - ptr_offset
                    ..pointer.offset as usize - ptr_offset + pointer.len.actual_len()],
            );
            free_space_offset -= pointer.len.actual_len();
            pointer.offset = free_space_offset as u16;
        }
        body[free_space_offset - ptr_offset..].copy_from_slice(&buf[free_space_offset..body_len]);

        self.header.free_space_offset = free_space_offset as u16;
        debug_assert_eq!(
            self.header.free_space,
            free_space_offset as u16 - size_of::<Pointer>() as u16 * self.header.num_slots
        );
        self.header.first_freed_block_offset = 0;
    }

    fn allocate(&mut self, len: usize, for_insert: bool) -> SimplePointer {
        // Best fit
        let mut freed_pointer: Option<FreedPointer> = None;
        for f in self
            .freed_blocks()
            .filter(|b| b.pointer.len as usize >= len)
        {
            if f.pointer.len as usize == len {
                freed_pointer = Some(f);
                break;
            }

            freed_pointer = match freed_pointer {
                None => Some(f),
                Some(c) if f.pointer.len < c.pointer.len => Some(f),
                c => c,
            };
        }
        if let Some(freed_pointer) = freed_pointer {
            let pointer = SimplePointer {
                offset: freed_pointer.pointer.offset + freed_pointer.pointer.len - len as u16,
                len: len as u16,
            };

            let rest_len = freed_pointer.pointer.len as usize - len;

            if rest_len >= size_of::<FreedBlock>() {
                let offset = freed_pointer.pointer.offset;
                FreedBlock::write_len(&mut self.body[offset as usize..], rest_len as u16);
            } else if freed_pointer.prev_freed_block_offset == 0 {
                self.header.first_freed_block_offset = freed_pointer.next_freed_block_offset;
            } else {
                FreedBlock::write_next_freed_offset(
                    &mut self.body[freed_pointer.prev_freed_block_offset as usize..],
                    freed_pointer.next_freed_block_offset,
                );
            }

            pointer
        } else {
            if self.free_space() < len + if for_insert { size_of::<Pointer>() } else { 0 } {
                self.defrag();
            }

            let pointer = SimplePointer {
                offset: self.header.free_space_offset - len as u16,
                len: len as u16,
            };

            self.header.free_space_offset -= len as u16;

            pointer
        }
    }

    async fn create_overflowed_pages(
        bytes: &[u8],
        free_list: &FreeList,
    ) -> Result<PageId, buffer::Error> {
        let mut remaining = bytes;

        let mut current_page = free_list.new_page().await?;
        let first_page_id = current_page.page_id;

        while !remaining.is_empty() {
            if remaining.len() <= disk::PAGE_SIZE {
                let mut lock = current_page.page.write().await;
                lock[..remaining.len()].copy_from_slice(remaining);
                remaining = &[];
            } else {
                let new_page = free_list.new_page().await?;
                let mut lock = current_page.page.write().await;
                let mut overflowed_page = OverFlowedPage::new(lock.as_bytes_mut());
                overflowed_page.header.next_page_id = new_page.page_id;
                overflowed_page
                    .body
                    .copy_from_slice(&remaining[..overflowed_page.body.len()]);
                remaining = &remaining[overflowed_page.body.len()..];
                drop(lock);
                current_page = new_page;
            }
        }

        Ok(first_page_id)
    }

    async fn remove_overflowed_pages(
        page_id: PageId,
        len: usize,
        free_list: &FreeList,
    ) -> Result<(), buffer::Error> {
        let mut current_page_id = page_id;
        let mut remaining_len = len;

        while remaining_len > 0 {
            if remaining_len <= disk::PAGE_SIZE {
                free_list.remove_page(current_page_id).await?;
                remaining_len = 0;
            } else {
                let page = free_list.fetch_page(current_page_id).await?;
                let lock = page.page.read().await;
                let overflowed_page = OverFlowedPage::new(lock.as_bytes());
                remaining_len -= overflowed_page.body.len();
                let remove_page_id = current_page_id;
                current_page_id = overflowed_page.header.next_page_id;
                drop(lock);
                drop(page);
                free_list.remove_page(remove_page_id).await?;
            }
        }

        Ok(())
    }

    fn insert_raw(&mut self, index: usize, is_overflow: bool, bytes: &[u8]) -> Option<()> {
        if self.free_capacity() < size_of::<Pointer>() + bytes.len() {
            return None;
        }

        if self.free_space() < size_of::<Pointer>() {
            self.defrag();
            debug_assert!(self.free_space() >= size_of::<Pointer>());
        }
        let pointer = self.allocate(bytes.len(), true);
        debug_assert_eq!(pointer.len, bytes.len() as u16);
        let num_slots_orig = self.num_slots();
        self.header.num_slots += 1;
        let mut pointers_mut = self.pointers_mut();
        pointers_mut.copy_within(index..num_slots_orig, index + 1);
        pointers_mut[index] = if is_overflow {
            Pointer {
                len: Length::OVERFLOWED,
                offset: pointer.offset,
            }
        } else {
            Pointer {
                len: Length(pointer.len),
                offset: pointer.offset,
            }
        };
        self.header.free_space -= bytes.len() as u16 + size_of::<Pointer>() as u16;
        self.body[pointer.range()].copy_from_slice(bytes);
        Some(())
    }

    pub async fn insert(
        &mut self,
        index: usize,
        bytes: &[u8],
        free_list: &FreeList,
    ) -> Result<Option<()>, buffer::Error> {
        let actual_len = Self::actual_len(bytes.len());

        if self.free_capacity() < size_of::<Pointer>() + actual_len {
            return Ok(None);
        }

        if self.free_space() < size_of::<Pointer>() {
            self.defrag();
            debug_assert!(self.free_space() >= size_of::<Pointer>());
        }
        let pointer = self.allocate(actual_len, true);
        debug_assert_eq!(pointer.len, actual_len as u16);
        let num_slots_orig = self.num_slots();
        self.header.num_slots += 1;
        let mut pointers_mut = self.pointers_mut();
        pointers_mut.copy_within(index..num_slots_orig, index + 1);
        pointers_mut[index] = if bytes.len() > Self::MAX_PAYLOAD_SIZE {
            Pointer {
                len: Length::OVERFLOWED,
                offset: pointer.offset,
            }
        } else {
            Pointer {
                len: Length(pointer.len),
                offset: pointer.offset,
            }
        };
        self.header.free_space -= actual_len as u16 + size_of::<Pointer>() as u16;

        if bytes.len() > Self::MAX_PAYLOAD_SIZE {
            let page_id = Self::create_overflowed_pages(bytes, free_list).await?;
            Overflow {
                len: bytes.len() as _,
                page_id,
            }
            .write(&mut self.body[pointer.range()]);
        } else {
            self.body[pointer.range()].copy_from_slice(bytes);
        }
        Ok(Some(()))
    }

    async fn remove_block(
        &mut self,
        index: usize,
        free_list: &FreeList,
    ) -> Result<(), buffer::Error> {
        let pointer = self.pointers()[index];
        if pointer.len.valid().is_none() {
            let overflow = Overflow::read(&self.body[pointer.range()]);
            Self::remove_overflowed_pages(overflow.page_id, overflow.len as usize, free_list)
                .await?;
        }
        if pointer.len.actual_len() >= size_of::<FreedBlock>() {
            FreedBlock {
                len: pointer.len.actual_len() as u16,
                next_freed_block_offset: self.header.first_freed_block_offset,
            }
            .write(&mut self.body[pointer.offset as usize..]);

            self.header.first_freed_block_offset = pointer.offset;
        }
        Ok(())
    }

    fn remove_block_raw(&mut self, index: usize) {
        let pointer = self.pointers()[index];
        if pointer.len.actual_len() >= size_of::<FreedBlock>() {
            FreedBlock {
                len: pointer.len.actual_len() as u16,
                next_freed_block_offset: self.header.first_freed_block_offset,
            }
            .write(&mut self.body[pointer.offset as usize..]);

            self.header.first_freed_block_offset = pointer.offset;
        }
    }

    pub async fn remove(
        &mut self,
        index: usize,
        free_list: &FreeList,
    ) -> Result<(), buffer::Error> {
        let pointer = self.pointers()[index];
        self.remove_block(index, free_list).await?;

        self.pointers_mut().copy_within(index + 1.., index);
        self.header.num_slots -= 1;
        self.header.free_space += pointer.len.actual_len() as u16 + size_of::<Pointer>() as u16;
        Ok(())
    }

    fn remove_raw(&mut self, index: usize) {
        let pointer = self.pointers()[index];
        self.remove_block_raw(index);

        self.pointers_mut().copy_within(index + 1.., index);
        self.header.num_slots -= 1;
        self.header.free_space += pointer.len.actual_len() as u16 + size_of::<Pointer>() as u16;
    }

    pub async fn replace(
        &mut self,
        index: usize,
        bytes: &[u8],
        free_list: &FreeList,
    ) -> Result<Option<()>, buffer::Error> {
        let pointer = self.pointers()[index];
        if pointer.len.actual_len() == Self::actual_len(bytes.len()) {
            if pointer.len.valid().is_none() {
                let overflow = Overflow::read(&self.body[pointer.range()]);
                Self::remove_overflowed_pages(overflow.page_id, overflow.len as usize, free_list)
                    .await?;
            }

            if bytes.len() > Self::MAX_PAYLOAD_SIZE {
                let page_id = Self::create_overflowed_pages(bytes, free_list).await?;
                Overflow {
                    page_id,
                    len: bytes.len() as u64,
                }
                .write(&mut self.body[pointer.range()]);
            } else {
                self.body[pointer.range()].copy_from_slice(bytes);
            }

            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    pub fn transfer(&mut self, dest: &mut SlottedOverflow<impl ByteSliceMut>) -> Option<()> {
        let pointer = self.pointers()[0];
        if dest
            .insert_raw(
                dest.num_slots(),
                pointer.len.valid().is_none(),
                &self.body[pointer.range()],
            )
            .is_some()
        {
            self.remove_raw(0);
            Some(())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use super::*;
    use buffer::BufferPoolManager;
    use disk::DiskManager;
    use rand::prelude::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();

        let page = free_list.new_page().await.unwrap();
        let mut page_lock = page.page.write().await;
        let mut slotted_overflow = SlottedOverflow::new(page_lock.as_bytes_mut());
        slotted_overflow.initialize();

        slotted_overflow
            .insert(0, b"hello", &free_list)
            .await
            .unwrap()
            .unwrap();
        slotted_overflow
            .insert(1, b"world", &free_list)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            slotted_overflow.fetch(0, &free_list).await.unwrap().deref(),
            b"hello"
        );

        assert_eq!(
            slotted_overflow.fetch(1, &free_list).await.unwrap().deref(),
            b"world"
        );
    }

    #[tokio::test]
    async fn test_large() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();

        let page = free_list.new_page().await.unwrap();
        let mut page_lock = page.page.write().await;
        let mut slotted_overflow = SlottedOverflow::new(page_lock.as_bytes_mut());
        slotted_overflow.initialize();

        let mut rng = StdRng::from_seed([0xDE; 32]);

        let data1 = {
            let mut v = vec![0u8; 1024 * 1024];
            rng.fill_bytes(&mut v);
            v
        };
        let data2 = {
            let mut v = vec![0u8; 1024 * 1024];
            rng.fill_bytes(&mut v);
            v
        };

        slotted_overflow
            .insert(0, &data1, &free_list)
            .await
            .unwrap()
            .unwrap();
        slotted_overflow
            .insert(1, &data2, &free_list)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            slotted_overflow.fetch(0, &free_list).await.unwrap().deref(),
            &data1
        );

        assert_eq!(
            slotted_overflow.fetch(1, &free_list).await.unwrap().deref(),
            &data2
        );
    }

    #[tokio::test]
    async fn test_remove() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();

        let page = free_list.new_page().await.unwrap();
        let mut page_lock = page.page.write().await;
        let mut slotted_overflow = SlottedOverflow::new(page_lock.as_bytes_mut());
        slotted_overflow.initialize();

        slotted_overflow
            .insert(0, b"hello", &free_list)
            .await
            .unwrap()
            .unwrap();
        slotted_overflow
            .insert(1, b"world", &free_list)
            .await
            .unwrap()
            .unwrap();

        slotted_overflow.remove(0, &free_list).await.unwrap();

        assert_eq!(
            slotted_overflow.fetch(0, &free_list).await.unwrap().deref(),
            b"world"
        );
    }

    #[tokio::test]
    async fn test_remove_large() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 16);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();

        let page = free_list.new_page().await.unwrap();
        let mut page_lock = page.page.write().await;
        let mut slotted_overflow = SlottedOverflow::new(page_lock.as_bytes_mut());
        slotted_overflow.initialize();

        let mut rng = StdRng::from_seed([0xDE; 32]);

        let data1 = {
            let mut v = vec![0u8; 1024 * 1024];
            rng.fill_bytes(&mut v);
            v
        };
        let data2 = {
            let mut v = vec![0u8; 1024 * 1024];
            rng.fill_bytes(&mut v);
            v
        };

        slotted_overflow
            .insert(0, &data1, &free_list)
            .await
            .unwrap()
            .unwrap();
        slotted_overflow
            .insert(1, &data2, &free_list)
            .await
            .unwrap()
            .unwrap();

        slotted_overflow.remove(0, &free_list).await.unwrap();

        assert_eq!(
            slotted_overflow.fetch(0, &free_list).await.unwrap().deref(),
            &data2
        );
    }

    fn free_capacity(memory: &[Vec<u8>]) -> usize {
        PAGE_SIZE
            - size_of::<Header>()
            - memory.len() * size_of::<Pointer>()
            - memory
                .iter()
                .map(|v| v.len())
                .map(|l| {
                    if l > SlottedOverflow::<[u8; 4096]>::MAX_PAYLOAD_SIZE {
                        size_of::<Overflow>()
                    } else {
                        l
                    }
                })
                .sum::<usize>()
    }

    #[tokio::test]
    async fn test_random() {
        let path = NamedTempFile::new().unwrap().into_temp_path();

        let disk_manager = DiskManager::open(&path).unwrap();
        let buffer_pool_manager = BufferPoolManager::new(disk_manager, 1024);
        let free_list = FreeList::create(buffer_pool_manager).await.unwrap();

        let page = free_list.new_page().await.unwrap();
        let mut page_lock = page.page.write().await;
        let mut slotted_overflow = SlottedOverflow::new(page_lock.as_bytes_mut());
        slotted_overflow.initialize();

        let mut rng = StdRng::from_seed([0xDE; 32]);

        let mut memory: Vec<Vec<u8>> = Vec::new();

        for _ in 0..512 {
            let p: f32 = rng.gen();
            match p {
                p if p < 0.25 => {
                    // inseert small
                    let data = {
                        let mut data =
                            vec![
                                0u8;
                                rng.gen_range(0..=SlottedOverflow::<[u8; 4096]>::MAX_PAYLOAD_SIZE)
                            ];
                        rng.fill_bytes(&mut data);
                        data
                    };

                    if data.len() + size_of::<Pointer>() <= slotted_overflow.free_capacity() {
                        let i = rng.gen_range(0..=memory.len());
                        slotted_overflow
                            .insert(i, &data, &free_list)
                            .await
                            .unwrap()
                            .unwrap();
                        memory.insert(i, data);
                    }
                }
                p if p < 0.50 => {
                    // insert large
                    let data = {
                        let mut data = vec![
                            0u8;
                            rng.gen_range(
                                SlottedOverflow::<[u8; 4096]>::MAX_PAYLOAD_SIZE + 1..16 * PAGE_SIZE
                            )
                        ];
                        rng.fill_bytes(&mut data);
                        data
                    };

                    if size_of::<Overflow>() + size_of::<Pointer>()
                        <= slotted_overflow.free_capacity()
                    {
                        let i = rng.gen_range(0..=memory.len());
                        slotted_overflow
                            .insert(i, &data, &free_list)
                            .await
                            .unwrap()
                            .unwrap();
                        memory.insert(i, data);
                    }
                }
                p if p < 0.75 => {
                    if let Some(i) = (0..memory.len()).choose(&mut rng) {
                        let new_data = {
                            let mut data = vec![0; memory[i].len()];
                            rng.fill_bytes(&mut data);
                            data
                        };
                        slotted_overflow
                            .replace(i, &new_data, &free_list)
                            .await
                            .unwrap()
                            .unwrap();
                        memory[i] = new_data;
                    }
                }
                _ => {
                    if let Some(i) = (0..memory.len()).choose(&mut rng) {
                        slotted_overflow.remove(i, &free_list).await.unwrap();
                        memory.remove(i);
                    }
                }
            }
            assert_eq!(slotted_overflow.free_capacity(), free_capacity(&memory));
            assert_eq!(slotted_overflow.num_slots(), memory.len());
            for (i, buf) in memory.iter().enumerate() {
                assert_eq!(
                    slotted_overflow.fetch(i, &free_list).await.unwrap().deref(),
                    buf.as_slice()
                );
            }
        }
    }
}
