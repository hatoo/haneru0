use std::{marker::PhantomData, mem::size_of};

use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, FromBytes, LayoutVerified};

#[derive(Debug, FromBytes, AsBytes)]
#[repr(C)]
pub struct Header {
    len: usize,
}

#[derive(FromBytes)]
#[repr(C)]
pub struct Stack<B, E> {
    header: LayoutVerified<B, Header>,
    body: B,
    _phantom: PhantomData<E>,
}

impl<B: ByteSlice, E: FromBytes + AsBytes> Stack<B, E> {
    pub fn new(bytes: B) -> Self {
        let (header, body) = LayoutVerified::new_from_prefix(bytes).unwrap();
        Self {
            header,
            body,
            _phantom: Default::default(),
        }
    }
}

impl<B: ByteSliceMut, E: FromBytes + AsBytes> Stack<B, E> {
    pub fn initialize(&mut self) {
        self.header.len = 0;
    }

    fn free_sapce(&self) -> usize {
        self.body.len() - self.header.len * size_of::<E>()
    }

    pub fn push(&mut self, element: &E) -> Option<()> {
        if self.free_sapce() < size_of::<E>() {
            return None;
        }
        self.body
            [self.header.len * size_of::<E>()..self.header.len * size_of::<E>() + size_of::<E>()]
            .copy_from_slice(element.as_bytes());
        Some(())
    }

    pub fn pop(&mut self) -> Option<LayoutVerified<&[u8], E>> {
        if self.header.len == 0 {
            return None;
        }

        self.header.len -= 1;
        Some(
            LayoutVerified::new_from_prefix(
                &self.body[self.header.len * size_of::<E>()
                    ..self.header.len * size_of::<E>() + size_of::<E>()],
            )
            .unwrap()
            .0,
        )
    }
}
