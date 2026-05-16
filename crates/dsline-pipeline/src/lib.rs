//! Pipeline runtime — composable operator chains.
//!
//! The 0.0.1 pipeline composes operators sequentially on a single thread.
//! A tokio-based multi-threaded executor with inter-stage bounded channels
//! is deferred until the 0.1.0 MPSC / transport backends land.

use dsline_core::error::Result;

// ── core traits ──

/// A source of items for a pipeline stage.
pub trait Stream {
    type Item;

    /// Pull the next item. `Ok(None)` signals end-of-stream.
    fn poll_next(&mut self) -> Result<Option<Self::Item>>;
}

/// A terminal consumer at the end of a pipeline.
pub trait Sink {
    type Item;

    /// Push one item.
    fn poll_send(&mut self, item: Self::Item) -> Result<()>;

    /// Called exactly once after the source is exhausted.
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// ── built-in stream / sink adapters ──

/// Wraps an `Iterator<Item = Result<T>>` as a `Stream`.
pub struct IterStream<I> {
    inner: I,
}

impl<I> IterStream<I> {
    pub fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<I, T> Stream for IterStream<I>
where
    I: Iterator<Item = Result<T>>,
{
    type Item = T;

    fn poll_next(&mut self) -> Result<Option<T>> {
        match self.inner.next() {
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

/// Collects items into a `Vec`.
pub struct CollectSink<T> {
    items: Vec<T>,
}

impl<T> CollectSink<T> {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn into_items(self) -> Vec<T> {
        self.items
    }
}

impl<T> Default for CollectSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + 'static> Sink for CollectSink<T> {
    type Item = T;

    fn poll_send(&mut self, item: T) -> Result<()> {
        self.items.push(item);
        Ok(())
    }
}

// ── pipeline ──

/// A `Pipeline<In, Out>` holds a composed operator `In → Vec<Out>`.
///
/// Build a pipeline with [`Pipeline::identity()`], chain operators via
/// [`pipe()`](Pipeline::pipe), and execute with [`run()`](Pipeline::run).
///
/// # Example
///
/// ```
/// use dsline_pipeline::{Pipeline, IterStream, CollectSink};
///
/// let source = IterStream::new(vec![1i32, 2, 3].into_iter().map(Ok));
/// let sink = CollectSink::new();
///
/// let result = Pipeline::<i32, i32>::identity()
///     .pipe(|x| Ok(vec![x * 2]))
///     .pipe(|x| Ok(if x > 3 { vec![x] } else { vec![] }))
///     .run(source, sink)
///     .unwrap();
///
/// assert_eq!(result.into_items(), vec![4, 6]);
/// ```
pub struct Pipeline<In, Out> {
    op: Box<dyn FnMut(In) -> Result<Vec<Out>> + Send>,
}

impl<T> Pipeline<T, T>
where
    T: Clone + Send + 'static,
{
    /// Create a pass-through pipeline (no operators).
    pub fn identity() -> Self {
        Self {
            op: Box::new(|x| Ok(vec![x])),
        }
    }
}

impl<In, Out> Pipeline<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    /// Append an operator that transforms each `Out` into zero or more
    /// `Next` items. Returns `Pipeline<In, Next>`.
    ///
    /// Operators compose sequentially: the output of one stage feeds the
    /// input of the next.
    pub fn pipe<F, Next>(self, mut next: F) -> Pipeline<In, Next>
    where
        F: FnMut(Out) -> Result<Vec<Next>> + Send + 'static,
        Next: Send + 'static,
    {
        let mut prev = self.op;
        Pipeline {
            op: Box::new(move |input: In| {
                let mut result = Vec::new();
                for mid in prev(input)? {
                    result.extend(next(mid)?);
                }
                Ok(result)
            }),
        }
    }

    /// Run the pipeline end-to-end, consuming source and sink.
    ///
    /// Blocks the calling thread until the source is exhausted, then
    /// calls `sink.close()`. Returns the sink for inspection.
    pub fn run<S, K>(mut self, mut source: S, mut sink: K) -> Result<K>
    where
        S: Stream<Item = In>,
        K: Sink<Item = Out>,
    {
        while let Some(item) = source.poll_next()? {
            for output in (self.op)(item)? {
                sink.poll_send(output)?;
            }
        }
        sink.close()?;
        Ok(sink)
    }
}

// ── tests ──

#[cfg(test)]
mod tests {
    use super::{CollectSink, IterStream, Pipeline};

    fn ok_stream<T: Send + 'static>(
        items: Vec<T>,
    ) -> IterStream<impl Iterator<Item = dsline_core::error::Result<T>>> {
        IterStream::new(items.into_iter().map(Ok))
    }

    #[test]
    fn identity_passthrough() {
        let source = ok_stream(vec![1, 2, 3]);
        let sink = CollectSink::new();

        let sink = Pipeline::<i32, i32>::identity().run(source, sink).unwrap();
        assert_eq!(sink.into_items(), vec![1, 2, 3]);
    }

    #[test]
    fn single_map_operator() {
        let source = ok_stream(vec![1, 2, 3]);
        let sink = CollectSink::new();

        let sink = Pipeline::<i32, i32>::identity()
            .pipe(|x| Ok(vec![x * 10]))
            .run(source, sink)
            .unwrap();

        assert_eq!(sink.into_items(), vec![10, 20, 30]);
    }

    #[test]
    fn map_then_filter() {
        let source = ok_stream(vec![1, 2, 3, 4, 5]);
        let sink = CollectSink::new();

        let sink = Pipeline::<i32, i32>::identity()
            .pipe(|x| Ok(vec![x * 2]))
            .pipe(|x| Ok(if x > 5 { vec![x] } else { vec![] }))
            .run(source, sink)
            .unwrap();

        assert_eq!(sink.into_items(), vec![6, 8, 10]);
    }

    #[test]
    fn flat_map_expansion() {
        let source = ok_stream(vec![1, 2]);
        let sink = CollectSink::new();

        let sink = Pipeline::<i32, i32>::identity()
            .pipe(|x| Ok((0..x).collect()))
            .run(source, sink)
            .unwrap();

        assert_eq!(sink.into_items(), vec![0, 0, 1]);
    }

    #[test]
    fn filter_everything_yields_empty() {
        let source = ok_stream(vec![1i32, 2]);
        let sink: CollectSink<i32> = CollectSink::new();

        let sink = Pipeline::<i32, i32>::identity()
            .pipe(|_x| Ok(vec![]))
            .run(source, sink)
            .unwrap();

        assert!(sink.into_items().is_empty());
    }

    #[test]
    fn type_change_across_operators() {
        let source = ok_stream(vec![1i32, 2]);
        let sink = CollectSink::new();

        let sink = Pipeline::<i32, i32>::identity()
            .pipe(|x| Ok(vec![x as u64 * 1000]))
            .pipe(|x: u64| Ok(vec![format!("v{}", x)]))
            .run(source, sink)
            .unwrap();

        assert_eq!(sink.into_items(), vec!["v1000", "v2000"]);
    }

    #[test]
    fn chained_operators_run_in_order() {
        let source = ok_stream(vec![1, 2, 3]);
        let sink = CollectSink::new();

        let sink = Pipeline::<i32, i32>::identity()
            .pipe(|x| Ok(vec![x + 1]))
            .pipe(|x| Ok(vec![x * 10]))
            .pipe(|x| Ok(vec![x - 5]))
            .run(source, sink)
            .unwrap();

        // (1+1)*10-5=15, (2+1)*10-5=25, (3+1)*10-5=35
        assert_eq!(sink.into_items(), vec![15, 25, 35]);
    }

    #[test]
    fn close_is_called_after_run() {
        let source = ok_stream(vec![1i32]);
        let sink = CollectSink::new();
        let sink = Pipeline::<i32, i32>::identity().run(source, sink).unwrap();
        assert_eq!(sink.into_items(), vec![1]);
    }
}
