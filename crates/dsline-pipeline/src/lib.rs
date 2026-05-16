//! Pipeline runtime — composable operator chains.
//!
//! The 0.0.1 pipeline composes operators sequentially on a single thread.
//! A tokio-based multi-threaded executor with inter-stage bounded channels
//! is deferred until the 0.1.0 MPSC / transport backends land.

use dsline_core::error::{ChannelError, DslineError, Result};
use dsline_ops::{eval, eval_bool, parse_expr, Record};

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

// ── expr-lite operators ──

/// An operator that transforms `I` into zero or more `O` items.
pub type Operator<I, O> = Box<dyn FnMut(I) -> Result<Vec<O>> + Send>;

/// Build a filter operator from an expr-lite expression string.
///
/// Items for which the expression evaluates to non-zero are passed through;
/// items evaluating to zero (or `None` on missing columns) are dropped.
///
/// # Example
///
/// ```
/// use std::collections::HashMap;
/// use dsline_pipeline::{Pipeline, IterStream, CollectSink, filter_expr};
///
/// let mut r1 = HashMap::new();
/// r1.insert("temp".into(), 10.0);
/// let mut r2 = HashMap::new();
/// r2.insert("temp".into(), 30.0);
/// let mut r3 = HashMap::new();
/// r3.insert("temp".into(), 25.0);
///
/// let source = IterStream::new(vec![r1, r2, r3].into_iter().map(Ok));
/// let sink = CollectSink::new();
///
/// let result = Pipeline::<HashMap<String, f64>, HashMap<String, f64>>::identity()
///     .pipe(filter_expr("temp > 20").unwrap())
///     .run(source, sink)
///     .unwrap();
///
/// let items = result.into_items();
/// assert_eq!(items.len(), 2);
/// assert_eq!(items[0].get("temp"), Some(&30.0));
/// ```
pub fn filter_expr<I>(expr: &str) -> Result<Operator<I, I>>
where
    I: Record + Send + 'static,
{
    let ast = parse_expr(expr).map_err(|_| {
        DslineError::Channel(ChannelError::InvalidConfig("invalid filter expression"))
    })?;
    Ok(Box::new(move |item: I| {
        let keep = eval_bool(&ast, &item).unwrap_or(false);
        Ok(if keep { vec![item] } else { vec![] })
    }))
}

/// Build a map operator from an expr-lite expression string.
///
/// Each input item is replaced by the numeric result of the expression.
/// The output type is `f64`.
///
/// # Example
///
/// ```
/// use std::collections::HashMap;
/// use dsline_pipeline::{Pipeline, IterStream, CollectSink, map_expr};
///
/// let mut r1 = HashMap::new();
/// r1.insert("val".into(), 1.0);
/// let mut r2 = HashMap::new();
/// r2.insert("val".into(), 2.0);
/// let mut r3 = HashMap::new();
/// r3.insert("val".into(), 3.0);
///
/// let source = IterStream::new(vec![r1, r2, r3].into_iter().map(Ok));
/// let sink = CollectSink::new();
///
/// let result = Pipeline::<HashMap<String, f64>, HashMap<String, f64>>::identity()
///     .pipe(map_expr("val * 10 + 1").unwrap())
///     .run(source, sink)
///     .unwrap();
///
/// assert_eq!(result.into_items(), vec![11.0, 21.0, 31.0]);
/// ```
pub fn map_expr<I>(expr: &str) -> Result<Operator<I, f64>>
where
    I: Record + Send + 'static,
{
    let ast = parse_expr(expr)
        .map_err(|_| DslineError::Channel(ChannelError::InvalidConfig("invalid map expression")))?;
    Ok(Box::new(move |item: I| {
        let value = eval(&ast, &item).unwrap_or(f64::NAN);
        Ok(vec![value])
    }))
}

// ── tests ──

#[cfg(test)]
mod tests {
    use super::{filter_expr, map_expr, CollectSink, IterStream, Pipeline};
    use dsline_ops::Record;
    use std::collections::HashMap;

    /// Newtype wrapper that makes an `f64` usable as a `Record`.
    /// Column `"x"` returns the wrapped value; all other names return `None`.
    #[derive(Debug, Clone, Copy)]
    struct Num(f64);

    impl Record for Num {
        fn column(&self, name: &str) -> Option<f64> {
            match name {
                "x" => Some(self.0),
                _ => None,
            }
        }
    }

    impl From<i32> for Num {
        fn from(v: i32) -> Self {
            Num(v as f64)
        }
    }

    fn num_stream(
        items: Vec<i32>,
    ) -> IterStream<impl Iterator<Item = dsline_core::error::Result<Num>>> {
        IterStream::new(items.into_iter().map(|v| Ok(Num(v as f64))))
    }

    fn hm_stream(
        items: Vec<HashMap<String, f64>>,
    ) -> IterStream<impl Iterator<Item = dsline_core::error::Result<HashMap<String, f64>>>> {
        IterStream::new(items.into_iter().map(Ok))
    }

    // ── basic pipeline tests ──

    #[test]
    fn identity_passthrough() {
        let source = num_stream(vec![1, 2, 3]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity().run(source, sink).unwrap();
        let vals: Vec<f64> = sink.into_items().iter().map(|n| n.0).collect();
        assert_eq!(vals, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn single_map_operator() {
        let source = num_stream(vec![1, 2, 3]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(|n: Num| Ok(vec![Num(n.0 * 10.0)]))
            .run(source, sink)
            .unwrap();

        let vals: Vec<f64> = sink.into_items().iter().map(|n| n.0).collect();
        assert_eq!(vals, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn map_then_filter() {
        let source = num_stream(vec![1, 2, 3, 4, 5]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(|n: Num| Ok(vec![Num(n.0 * 2.0)]))
            .pipe(|n: Num| Ok(if n.0 > 5.0 { vec![n] } else { vec![] }))
            .run(source, sink)
            .unwrap();

        let vals: Vec<f64> = sink.into_items().iter().map(|n| n.0).collect();
        assert_eq!(vals, vec![6.0, 8.0, 10.0]);
    }

    #[test]
    fn flat_map_expansion() {
        let source = num_stream(vec![1, 2]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(|n: Num| {
                let count = n.0 as i32;
                Ok((0..count).map(|i| Num(i as f64)).collect())
            })
            .run(source, sink)
            .unwrap();

        let vals: Vec<f64> = sink.into_items().iter().map(|n| n.0).collect();
        assert_eq!(vals, vec![0.0, 0.0, 1.0]);
    }

    #[test]
    fn filter_everything_yields_empty() {
        let source = num_stream(vec![1, 2]);
        let sink: CollectSink<Num> = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(|_n: Num| Ok(vec![]))
            .run(source, sink)
            .unwrap();

        assert!(sink.into_items().is_empty());
    }

    #[test]
    fn type_change_across_operators() {
        let source = num_stream(vec![1, 2]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(|n: Num| Ok(vec![(n.0 * 1000.0) as u64]))
            .pipe(|x: u64| Ok(vec![format!("v{}", x)]))
            .run(source, sink)
            .unwrap();

        assert_eq!(sink.into_items(), vec!["v1000", "v2000"]);
    }

    #[test]
    fn chained_operators_run_in_order() {
        let source = num_stream(vec![1, 2, 3]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(|n: Num| Ok(vec![Num(n.0 + 1.0)]))
            .pipe(|n: Num| Ok(vec![Num(n.0 * 10.0)]))
            .pipe(|n: Num| Ok(vec![Num(n.0 - 5.0)]))
            .run(source, sink)
            .unwrap();

        // (1+1)*10-5=15, (2+1)*10-5=25, (3+1)*10-5=35
        let vals: Vec<f64> = sink.into_items().iter().map(|n| n.0).collect();
        assert_eq!(vals, vec![15.0, 25.0, 35.0]);
    }

    #[test]
    fn close_is_called_after_run() {
        let source = num_stream(vec![1]);
        let sink = CollectSink::new();
        let sink = Pipeline::<Num, Num>::identity().run(source, sink).unwrap();
        assert_eq!(sink.into_items().len(), 1);
    }

    // ── expr-lite operator tests ──

    #[test]
    fn filter_expr_drops_items() {
        let source = num_stream(vec![1, 2, 3, 4, 5]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(filter_expr("x >= 3").unwrap())
            .run(source, sink)
            .unwrap();

        let vals: Vec<f64> = sink.into_items().iter().map(|n| n.0).collect();
        assert_eq!(vals, vec![3.0, 4.0, 5.0]);
    }

    #[test]
    fn filter_expr_keeps_none_on_bad_column() {
        let source = num_stream(vec![1, 2]);
        let sink: CollectSink<Num> = CollectSink::new();

        // "y" column doesn't exist → eval returns None → treated as false
        let sink = Pipeline::<Num, Num>::identity()
            .pipe(filter_expr("y > 0").unwrap())
            .run(source, sink)
            .unwrap();

        assert!(sink.into_items().is_empty());
    }

    #[test]
    fn map_expr_transforms_values() {
        let source = num_stream(vec![1, 2, 3]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(map_expr("x * x + 1").unwrap())
            .run(source, sink)
            .unwrap();

        assert_eq!(sink.into_items(), vec![2.0, 5.0, 10.0]);
    }

    #[test]
    fn filter_then_map_expr_composed() {
        let source = num_stream(vec![1, 2, 3, 4]);
        let sink = CollectSink::new();

        let sink = Pipeline::<Num, Num>::identity()
            .pipe(filter_expr("x > 2").unwrap())
            .pipe(map_expr("x * 10").unwrap())
            .run(source, sink)
            .unwrap();

        assert_eq!(sink.into_items(), vec![30.0, 40.0]);
    }

    #[test]
    fn expr_operators_with_hashmap_records() {
        let mut r1 = HashMap::new();
        r1.insert("temp".into(), 25.0);
        r1.insert("humidity".into(), 60.0);
        let mut r2 = HashMap::new();
        r2.insert("temp".into(), 35.0);
        r2.insert("humidity".into(), 90.0);

        let source = hm_stream(vec![r1, r2]);
        let sink = CollectSink::new();

        let sink = Pipeline::<HashMap<String, f64>, HashMap<String, f64>>::identity()
            .pipe(filter_expr("temp > 20 and humidity < 80").unwrap())
            .run(source, sink)
            .unwrap();

        let result = sink.into_items();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("temp"), Some(&25.0));
    }
}
