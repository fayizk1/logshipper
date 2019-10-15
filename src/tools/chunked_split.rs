use std::cmp;

#[derive(Debug)]
pub struct ChunkedSplit<'a, T: 'a> {
    v: &'a [T],
    sep: T,
    chunk_size: usize,
}

impl<'a, T> ChunkedSplit<'a, T> {
    pub fn len(&self) -> usize {
        self.v.len()
    }
}

pub trait ChunkedSplitTrait<T> {
    fn chucked_split(&self, sep: T, chunk_size: usize) -> ChunkedSplit<'_, T>;
}

impl<T> ChunkedSplitTrait<T> for Vec<T> {
    fn chucked_split(&self, sep: T, chunk_size: usize) -> ChunkedSplit<'_, T> {
        assert!(chunk_size != 0);
        ChunkedSplit {
            v: self,
            sep,
            chunk_size,
        }
    }
}

impl<'a, T> Iterator for ChunkedSplit<'a, T>
where
    T: Clone + Eq + std::fmt::Display,
{
    type Item = &'a [T];
    fn next(&mut self) -> Option<&'a [T]> {
        if self.v.is_empty() {
            None
        } else {
            let chunksz = cmp::min(self.v.len(), self.chunk_size);
            let (fst, _) = self.v.split_at(chunksz);
            let last_pos = match fst.iter().rposition(|x| *x == self.sep) {
                Some(s) => {
                    if s == 0 {
                        chunksz
                    } else {
                        s
                    }
                }
                None => fst.len(),
            };
            let (fst, rem) = self.v.split_at(last_pos);
            self.v = rem;
            Some(fst)
        }
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.v.is_empty() {
            (0, Some(0))
        } else {
            let n = self.v.len() / self.chunk_size;
            let rem = self.v.len() % self.chunk_size;
            let n = if rem > 0 { n + 1 } else { n };
            (n, Some(n))
        }
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }
    /*
    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let (start, overflow) = n.overflowing_mul(self.chunk_size);
        if start >= self.v.len() || overflow {
            self.v = &[];
            None
        } else {
            let end = match start.checked_add(self.chunk_size) {
                Some(sum) => cmp::min(self.v.len(), sum),
                None => self.v.len(),
            };
            let nth = &self.v[start..end];
            self.v = &self.v[end..];
            Some(nth)
        }
    }

    #[inline]
    fn last(self) -> Option<Self::Item> {
        if self.v.is_empty() {
            None
        } else {
            let start = (self.v.len() - 1) / self.chunk_size * self.chunk_size;
            Some(&self.v[start..])
        }
    }
    */
}
