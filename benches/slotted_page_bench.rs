use criterion::{criterion_group, criterion_main, Criterion};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("slotted_page_haneru", |b| {
        b.iter(|| {
            use haneru::btree::slotted::Slotted;
            use rand::prelude::*;

            let mut rng = StdRng::from_seed([114; 32]);
            let mut page_data = [0u8; 4096];
            let mut slotted = Slotted::new(&mut page_data[..]);
            slotted.initialize();

            for _ in 0..114514 {
                let p: f32 = rng.gen();
                match p {
                    _ if p < 0.50 => {
                        let index = rng.gen_range(0..=slotted.num_slots());
                        slotted.insert(index, rng.gen_range(0..32));
                    }
                    _ if p < 0.75 => {
                        if slotted.num_slots() > 0 {
                            let index = rng.gen_range(0..slotted.num_slots());
                            slotted.resize(index, rng.gen_range(0..32));
                        }
                    }
                    _ => {
                        if slotted.num_slots() > 0 {
                            let index = rng.gen_range(0..slotted.num_slots());
                            slotted.remove(index);
                        }
                    }
                }
            }
        })
    });

    c.bench_function("slotted_page_relly", |b| {
        b.iter(|| {
            /// Copied from https://github.com/KOBA789/relly/blob/5154fada683fb2ea1f1b0b100f3d5cbe2e3d6f42/src/slotted.rs
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
            use std::mem::size_of;
            use std::ops::{Index, IndexMut, Range};

            use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

            #[derive(Debug, FromBytes, AsBytes)]
            #[repr(C)]
            pub struct Header {
                num_slots: u16,
                free_space_offset: u16,
                _pad: u32,
            }

            #[derive(Debug, FromBytes, AsBytes, Clone, Copy)]
            #[repr(C)]
            pub struct Pointer {
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

            pub type Pointers<B> = LayoutVerified<B, [Pointer]>;

            pub struct Slotted<B> {
                header: LayoutVerified<B, Header>,
                body: B,
            }

            impl<B: ByteSlice> Slotted<B> {
                pub fn new(bytes: B) -> Self {
                    let (header, body) = LayoutVerified::new_from_prefix(bytes)
                        .expect("slotted header must be aligned");
                    Self { header, body }
                }

                pub fn capacity(&self) -> usize {
                    self.body.len()
                }

                pub fn num_slots(&self) -> usize {
                    self.header.num_slots as usize
                }

                pub fn free_space(&self) -> usize {
                    self.header.free_space_offset as usize - self.pointers_size()
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
                }

                fn pointers_mut(&mut self) -> Pointers<&mut [u8]> {
                    let pointers_size = self.pointers_size();
                    Pointers::new_slice(&mut self.body[..pointers_size]).unwrap()
                }

                fn data_mut(&mut self, pointer: Pointer) -> &mut [u8] {
                    &mut self.body[pointer.range()]
                }

                pub fn insert(&mut self, index: usize, len: usize) -> Option<()> {
                    if self.free_space() < size_of::<Pointer>() + len {
                        return None;
                    }
                    let num_slots_orig = self.num_slots();
                    self.header.free_space_offset -= len as u16;
                    self.header.num_slots += 1;
                    let free_space_offset = self.header.free_space_offset;
                    let mut pointers_mut = self.pointers_mut();
                    pointers_mut.copy_within(index..num_slots_orig, index + 1);
                    let pointer = &mut pointers_mut[index];
                    pointer.offset = free_space_offset;
                    pointer.len = len as u16;
                    Some(())
                }

                pub fn remove(&mut self, index: usize) {
                    self.resize(index, 0);
                    self.pointers_mut().copy_within(index + 1.., index);
                    self.header.num_slots -= 1;
                }

                pub fn resize(&mut self, index: usize, len_new: usize) -> Option<()> {
                    let pointers = self.pointers();
                    let len_orig = pointers[index].len;
                    let len_incr = len_new as isize - len_orig as isize;
                    if len_incr == 0 {
                        return Some(());
                    }
                    if len_incr > self.free_space() as isize {
                        return None;
                    }
                    let free_space_offset = self.header.free_space_offset as usize;
                    let offset_orig = pointers[index].offset;
                    let shift_range = free_space_offset..offset_orig as usize;
                    let free_space_offset_new = (free_space_offset as isize - len_incr) as usize;
                    self.header.free_space_offset = free_space_offset_new as u16;
                    self.body
                        .as_bytes_mut()
                        .copy_within(shift_range, free_space_offset_new);
                    let mut pointers_mut = self.pointers_mut();
                    for pointer in pointers_mut.iter_mut() {
                        if pointer.offset <= offset_orig {
                            pointer.offset = (pointer.offset as isize - len_incr) as u16;
                        }
                    }
                    let pointer = &mut pointers_mut[index];
                    pointer.len = len_new as u16;
                    if len_new == 0 {
                        pointer.offset = free_space_offset_new as u16;
                    }
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

            use rand::prelude::*;

            let mut rng = StdRng::from_seed([114; 32]);
            let mut page_data = [0u8; 4096];
            let mut slotted = Slotted::new(&mut page_data[..]);
            slotted.initialize();

            for _ in 0..114514 {
                let p: f32 = rng.gen();
                match p {
                    _ if p < 0.50 => {
                        let index = rng.gen_range(0..=slotted.num_slots());
                        slotted.insert(index, rng.gen_range(0..32));
                    }
                    _ if p < 0.75 => {
                        if slotted.num_slots() > 0 {
                            let index = rng.gen_range(0..slotted.num_slots());
                            slotted.resize(index, rng.gen_range(0..32));
                        }
                    }
                    _ => {
                        if slotted.num_slots() > 0 {
                            let index = rng.gen_range(0..slotted.num_slots());
                            slotted.remove(index);
                        }
                    }
                }
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
