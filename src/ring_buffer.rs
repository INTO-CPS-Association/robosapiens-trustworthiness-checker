use std::iter;
use std::mem;

#[derive(Debug, Clone)]
pub struct RingBuffer<T> {
    buffer: Vec<Option<T>>,
    start: usize,
    end: usize,
    capacity: usize,
    size: usize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        RingBuffer {
            buffer: (0..capacity).map(|_| None).collect(),
            start: 0,
            end: 0,
            capacity,
            size: 0,
        }
    }

    pub fn push(&mut self, item: T) -> Option<T> {
        if self.size == self.capacity {
            Some(item)
        } else {
            let new_index = if self.size == 0 {
                self.start
            } else {
                (self.end + 1) % self.capacity
            };
            assert!(new_index < self.capacity);
            // assert!(new_index != self.start);
            self.buffer[new_index] = Some(item);
            self.end = new_index;
            self.size += 1;
            None
        }
    }

    pub fn push_and_replace(&mut self, item: T) -> Option<T> {
        if self.size == self.capacity {
            let res = mem::take(&mut self.buffer[self.start]);
            assert!(res.is_some());
            self.buffer[self.start] = Some(item);
            res
        } else {
            self.push(item)
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.size == 0 {
            None
        } else {
            let item = mem::take(self.buffer.get_mut(self.start).unwrap());
            self.start = (self.start + 1) % self.capacity;
            self.size -= 1;
            Some(item.unwrap())
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn peek(&self) -> Option<&T> {
        self.buffer.get(self.start)?.as_ref()
    }

    pub fn into_iter(mut self) -> impl Iterator<Item = T> {
        iter::from_fn(move || match self.pop() {
            Some(item) => Some(item),
            None => None,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        let mut i = 0;
        iter::from_fn(move || {
            if i >= self.size {
                None
            } else {
                let index = i % self.capacity;
                let item = self.buffer[index].as_ref().unwrap();
                i += 1;
                Some(item)
            }
        })
    }
}

// struct RingBufferIterMut<'a, T: Default> {
//     buffer: &'a mut RingBuffer<T>,
//     index: usize,
// }

// impl<'a, T: Default> Iterator for RingBufferIterMut<'a, T> {
//     type Item = &'a mut T;

//     fn next(&'b mut self) -> Option<&'a + 'b mut T> {
//         if self.index == self.buffer.end {
//             None
//         } else {
//             let item: &'a mut T = self.buffer.buffer.get_mut(self.index).unwrap();
//             self.index = (self.index + 1) % self.buffer.size;
//             Some(item)
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer() {
        let mut buffer = RingBuffer::new(3);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.push(1), None);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.push(2), None);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.push(3), None);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.push(4), Some(4));
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.iter().collect::<Vec<_>>(), [&1, &2, &3]);
        assert_eq!(buffer.pop(), Some(1));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.push(4), None);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.pop(), Some(2));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.pop(), Some(3));
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.pop(), Some(4));
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn test_ring_buffer_replacing() {
        let mut buffer = RingBuffer::new(3);
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.push_and_replace(1), None);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.push_and_replace(2), None);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.push_and_replace(3), None);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.push_and_replace(4), Some(1));
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.iter().collect::<Vec<_>>(), [&4, &2, &3]);
        assert_eq!(buffer.pop(), Some(4));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.push_and_replace(4), None);
        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.pop(), Some(2));
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.pop(), Some(3));
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.pop(), Some(4));
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn test_ring_buffer_under_length() {
        let mut buffer = RingBuffer::new(3);
        buffer.push_and_replace(1);
        buffer.push_and_replace(2);
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.iter().collect::<Vec<_>>(), [&1, &2]);
    }
}
