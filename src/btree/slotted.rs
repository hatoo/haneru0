/// Based on https://github.com/KOBA789/relly/blob/5154fada683fb2ea1f1b0b100f3d5cbe2e3d6f42/src/slotted.rs
/// Copyright (c) 2021 Hidekazu Kobayashi <kobahide789@gmail.com>

/// Permission is hereby granted, free of charge, to any person obtaining a copy
/// of this software and associated documentation files (the "Software"), to deal
/// in the Software without restriction, including without limitation the rights
/// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
/// copies of the Software, and to permit persons to whom the Software is
/// furnished to do so, subject to the following conditions:
///
/// The above copyright notice and this permission notice shall be included in all
/// copies or substantial portions of the Software.
///
/// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
/// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
/// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
/// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
/// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
/// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
/// OR OTHER DEALINGS IN THE SOFTWARE.
use std::mem::{size_of, MaybeUninit};
use std::ops::{Index, IndexMut, Range};

use byteorder::{ByteOrder, NativeEndian};
use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

use crate::disk::PAGE_SIZE;

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

#[derive(Debug, FromBytes, AsBytes, Clone, Copy)]
#[repr(C)]
struct Pointer {
    offset: u16,
    len: u16,
}

impl Pointer {
    fn range(&self) -> Range<usize> {
        let start = self.offset as usize;
        let end = start + self.len as usize;
        start..end
    }
}

type Pointers<B> = LayoutVerified<B, [Pointer]>;

pub struct Slotted<B> {
    header: LayoutVerified<B, Header>,
    body: B,
}

struct FreedBlockIter<'a, B> {
    body: &'a B,
    prev: u16,
    current: u16,
}

#[derive(Debug)]
struct FreedPointer {
    pointer: Pointer,
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
                pointer: Pointer {
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

impl<B: ByteSlice> Slotted<B> {
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

    fn data(&self, pointer: Pointer) -> &[u8] {
        &self.body[pointer.range()]
    }
}

impl<B: ByteSliceMut> Slotted<B> {
    pub fn initialize(&mut self) {
        self.header.num_slots = 0;
        self.header.free_space_offset = self.body.len() as u16;
        self.header.free_space = self.body.len() as u16;
    }

    fn pointers_mut(&mut self) -> Pointers<&mut [u8]> {
        let pointers_size = self.pointers_size();
        Pointers::new_slice(&mut self.body[..pointers_size]).unwrap()
    }

    fn data_mut(&mut self, pointer: Pointer) -> &mut [u8] {
        &mut self.body[pointer.range()]
    }

    fn defrag(&mut self) {
        let body_len = self.body.len();
        let mut free_space_offset = body_len;
        let ptr_offset = size_of::<Pointer>() * self.num_slots();
        let mut buf: [u8; PAGE_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
        let (pointers, body) = self.body.split_at_mut(ptr_offset);
        let mut pointers = Pointers::new_slice(pointers).unwrap();
        for pointer in pointers.iter_mut() {
            buf[free_space_offset - pointer.len as usize..free_space_offset].copy_from_slice(
                &body[pointer.offset as usize - ptr_offset
                    ..pointer.offset as usize - ptr_offset + pointer.len as usize],
            );
            free_space_offset -= pointer.len as usize;
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

    fn allocate(&mut self, len: usize, for_insert: bool) -> Pointer {
        // Best fit
        if let Some(freed_pointer) = self
            .freed_blocks()
            .filter(|b| b.pointer.len as usize >= len)
            .min_by_key(|b| b.pointer.len)
        {
            let pointer = Pointer {
                offset: freed_pointer.pointer.offset + freed_pointer.pointer.len - len as u16,
                len: len as u16,
            };

            let rest_len = freed_pointer.pointer.len as usize - len;

            if rest_len >= size_of::<FreedBlock>() {
                let offset = freed_pointer.pointer.offset;
                FreedBlock::write_len(&mut self.body[offset as usize..], rest_len as u16);
            } else {
                if freed_pointer.prev_freed_block_offset == 0 {
                    self.header.first_freed_block_offset = freed_pointer.next_freed_block_offset;
                } else {
                    FreedBlock::write_next_freed_offset(
                        &mut self.body[freed_pointer.prev_freed_block_offset as usize..],
                        freed_pointer.next_freed_block_offset,
                    );
                }
            }
            pointer
        } else {
            if self.free_space() < len + if for_insert { size_of::<Pointer>() } else { 0 } {
                self.defrag();
            }

            let pointer = Pointer {
                offset: self.header.free_space_offset - len as u16,
                len: len as u16,
            };

            self.header.free_space_offset -= len as u16;

            pointer
        }
    }

    pub fn insert(&mut self, index: usize, len: usize) -> Option<()> {
        if self.free_capacity() < size_of::<Pointer>() + len {
            return None;
        }

        if self.free_space() < size_of::<Pointer>() {
            self.defrag();
            debug_assert!(self.free_space() >= size_of::<Pointer>());
        }
        let pointer = self.allocate(len, true);
        debug_assert_eq!(pointer.len, len as u16);
        let num_slots_orig = self.num_slots();
        self.header.num_slots += 1;
        let mut pointers_mut = self.pointers_mut();
        pointers_mut.copy_within(index..num_slots_orig, index + 1);
        pointers_mut[index] = pointer;
        self.header.free_space -= len as u16 + size_of::<Pointer>() as u16;
        Some(())
    }

    fn remove_block(&mut self, index: usize) {
        let pointer = self.pointers()[index];
        if pointer.len as usize >= size_of::<FreedBlock>() {
            FreedBlock {
                len: pointer.len,
                next_freed_block_offset: self.header.first_freed_block_offset,
            }
            .write(&mut self.body[pointer.offset as usize..]);

            self.header.first_freed_block_offset = pointer.offset;
        }
    }

    pub fn remove(&mut self, index: usize) {
        let pointer = self.pointers()[index];
        self.remove_block(index);

        self.pointers_mut().copy_within(index + 1.., index);
        self.header.num_slots -= 1;
        self.header.free_space += pointer.len + size_of::<Pointer>() as u16;
    }

    pub fn resize(&mut self, index: usize, len_new: usize) -> Option<()> {
        let len_orig = self.pointers()[index].len;
        let len_incr = len_new as isize - len_orig as isize;
        if len_incr == 0 {
            return Some(());
        }
        if len_incr > self.free_capacity() as isize {
            return None;
        }

        self.remove_block(index);
        self.pointers_mut()[index].len = 0;
        self.header.free_space += len_orig;
        self.pointers_mut()[index] = self.allocate(len_new, false);
        self.header.free_space -= len_new as u16;
        Some(())
    }
}

impl<B: ByteSlice> Index<usize> for Slotted<B> {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        self.data(self.pointers()[index])
    }
}

impl<B: ByteSliceMut> IndexMut<usize> for Slotted<B> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.data_mut(self.pointers()[index])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    #[test]
    fn test() {
        let mut page_data = vec![0u8; 128];
        let mut slotted = Slotted::new(page_data.as_mut_slice());
        let insert = |slotted: &mut Slotted<&mut [u8]>, index: usize, buf: &[u8]| {
            slotted.insert(index, buf.len()).unwrap();
            slotted[index].copy_from_slice(buf);
        };
        let push = |slotted: &mut Slotted<&mut [u8]>, buf: &[u8]| {
            let index = slotted.num_slots() as usize;
            insert(slotted, index, buf);
        };
        slotted.initialize();
        push(&mut slotted, b"hello");
        push(&mut slotted, b"world");
        assert_eq!(&slotted[0], b"hello");
        assert_eq!(&slotted[1], b"world");
        insert(&mut slotted, 1, b", ");
        push(&mut slotted, b"!");
        assert_eq!(&slotted[0], b"hello");
        assert_eq!(&slotted[1], b", ");
        assert_eq!(&slotted[2], b"world");
        assert_eq!(&slotted[3], b"!");
        slotted.defrag();
    }

    #[test]
    fn test_remove() {
        let mut page_data = vec![0u8; 128];
        let mut slotted = Slotted::new(page_data.as_mut_slice());
        let insert = |slotted: &mut Slotted<&mut [u8]>, index: usize, buf: &[u8]| {
            slotted.insert(index, buf.len()).unwrap();
            slotted[index].copy_from_slice(buf);
        };
        let push = |slotted: &mut Slotted<&mut [u8]>, buf: &[u8]| {
            let index = slotted.num_slots() as usize;
            insert(slotted, index, buf);
        };
        slotted.initialize();
        push(&mut slotted, b"hello");
        push(&mut slotted, b"world");
        assert_eq!(&slotted[0], b"hello");
        assert_eq!(&slotted[1], b"world");
        slotted.remove(0);
        assert_eq!(&slotted[0], b"world");
        slotted.defrag();
    }

    #[test]
    fn test_random() {
        let mut rng = thread_rng();
        let mut page_data = [0u8; 128];
        let mut slotted = Slotted::new(&mut page_data[..]);
        slotted.initialize();

        let mut memory = Vec::new();

        let insert = |slotted: &mut Slotted<&mut [u8]>, index: usize, buf: &[u8]| {
            let orig_free_capacity = slotted.free_capacity();
            slotted.insert(index, buf.len()).unwrap();
            assert_eq!(
                slotted.free_capacity(),
                orig_free_capacity - buf.len() - size_of::<Pointer>()
            );
            slotted[index].copy_from_slice(buf);
        };

        let replace = |slotted: &mut Slotted<&mut [u8]>, index: usize, buf: &[u8]| {
            slotted.resize(index, buf.len());
            slotted[index].copy_from_slice(buf);
        };

        let push = |slotted: &mut Slotted<&mut [u8]>, buf: &[u8]| {
            let index = slotted.num_slots() as usize;
            insert(slotted, index, buf);
        };

        for _ in 0..4096 {
            let p: f32 = rng.gen();
            match p {
                _ if p < 0.25 => {
                    let mut buf = vec![0u8; rng.gen_range(0..32)];
                    rng.fill_bytes(buf.as_mut_slice());

                    if slotted.free_capacity() >= buf.len() + size_of::<Pointer>() {
                        push(&mut slotted, buf.as_slice());
                        memory.push(buf);
                    }
                }
                _ if p < 0.50 => {
                    if memory.len() > 0 {
                        let i = rng.gen_range(0..memory.len());
                        let mut buf = vec![0u8; rng.gen_range(0..32)];
                        rng.fill_bytes(buf.as_mut_slice());

                        if slotted.free_capacity() >= buf.len() + size_of::<Pointer>() {
                            insert(&mut slotted, i, buf.as_slice());
                            memory.insert(i, buf);
                        }
                    }
                }
                _ if p < 0.75 => {
                    if memory.len() > 0 {
                        let i = rng.gen_range(0..memory.len());
                        let mut buf = vec![0u8; rng.gen_range(0..32)];
                        rng.fill_bytes(buf.as_mut_slice());

                        if slotted.free_capacity() as isize
                            >= buf.len() as isize - memory[i].len() as isize
                        {
                            replace(&mut slotted, i, buf.as_slice());
                            memory[i] = buf;
                        }
                    }
                }
                _ => {
                    if memory.len() > 0 {
                        let i = rng.gen_range(0..memory.len());
                        let orig_free_capacity = slotted.free_capacity();
                        let len = memory[i].len();
                        memory.remove(i);
                        slotted.remove(i);
                        assert_eq!(
                            slotted.free_capacity(),
                            orig_free_capacity + len + size_of::<Pointer>()
                        );
                    }
                }
            }
            assert_eq!(slotted.num_slots(), memory.len());
            for (i, buf) in memory.iter().enumerate() {
                assert_eq!(&slotted[i], buf.as_slice());
            }
        }
    }
}
