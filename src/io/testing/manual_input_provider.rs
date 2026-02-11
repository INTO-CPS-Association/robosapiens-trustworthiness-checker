use crate::{
    InputProvider, OutputStream, VarName, semantics::AsyncConfig,
    stream_utils::channel_to_output_stream,
};
use async_trait::async_trait;
use futures::future::LocalBoxFuture;
use futures::{StreamExt, stream};
use std::collections::BTreeMap;
use std::future::pending;
use unsync::spsc::Sender as SpscSender;

const CHANNEL_SIZE: usize = 10;

struct Channel<AC: AsyncConfig> {
    sender: Option<SpscSender<AC::Val>>,
    receiver: Option<OutputStream<AC::Val>>,
}

pub struct ManualInputProvider<AC: AsyncConfig> {
    vars: BTreeMap<VarName, Channel<AC>>,
}

impl<AC: AsyncConfig> ManualInputProvider<AC> {
    // This needs to be a way for the reconf runtime to instantiate the inner semi_sync RT.
    // So the reconf has a normal InputProvider and when constructing inner, it gives it this.
    //
    // On top of the regular InputProvider interface, it needs to have a complementary for pushing
    // values.
    pub fn new(input_vars: Vec<VarName>) -> Self {
        let vars: BTreeMap<VarName, Channel<AC>> = input_vars
            .into_iter()
            .map(|v| {
                let (tx, rx) = unsync::spsc::channel(CHANNEL_SIZE);
                let rx = channel_to_output_stream(rx);
                (
                    v,
                    Channel {
                        sender: Some(tx),
                        receiver: Some(rx),
                    },
                )
            })
            .collect();

        Self { vars }
    }

    pub fn sender_channel(&mut self, var: &VarName) -> Option<SpscSender<AC::Val>> {
        self.vars.get_mut(var)?.sender.take()
    }
}

#[async_trait(?Send)]
impl<AC: AsyncConfig> InputProvider for ManualInputProvider<AC> {
    type Val = AC::Val;
    fn var_stream(&mut self, var: &VarName) -> Option<OutputStream<Self::Val>> {
        self.vars
            .get_mut(var)
            .and_then(|channel| channel.receiver.take())
    }

    // TODO: Refactor such that the input_streams only forward data whenever run is called.
    async fn control_stream(&mut self) -> OutputStream<anyhow::Result<()>> {
        stream::repeat_with(|| Ok(())).boxed_local()
    }

    fn run(&mut self) -> LocalBoxFuture<'static, anyhow::Result<()>> {
        Box::pin(pending())
    }

    fn vars(&self) -> Vec<VarName> {
        self.vars.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        InputProvider, Value, async_test, io::testing::ManualInputProvider,
        lola_fixtures::TestConfig,
    };
    use futures::StreamExt;
    use macro_rules_attribute::apply;
    use smol::LocalExecutor;
    use std::rc::Rc;

    #[apply(async_test)]
    async fn test_send_receive(executor: Rc<LocalExecutor<'static>>) -> anyhow::Result<()> {
        let xs = vec![Value::Int(1), Value::Int(2)];
        let ys = vec![Value::Int(3), Value::Int(4)];
        let stream_len = xs.len();
        let var_names = vec!["x".into(), "y".into()];
        let mut input_provider = ManualInputProvider::<TestConfig>::new(var_names.clone());
        let mut x_stream = input_provider
            .var_stream(&"x".into())
            .expect("x stream should exist");
        let mut y_stream = input_provider
            .var_stream(&"y".into())
            .expect("y stream should exist");
        let mut x_sender = input_provider
            .sender_channel(&"x".into())
            .expect("x sender should exist");
        let mut y_sender = input_provider
            .sender_channel(&"y".into())
            .expect("y sender should exist");

        // For completeness:
        executor.spawn(input_provider.run()).detach();

        // Send values
        for i in 0..stream_len {
            x_sender
                .send(xs[i].clone())
                .await
                .expect("Failed to send x value");
            y_sender
                .send(ys[i].clone())
                .await
                .expect("Failed to send y value");
        }

        // Receive values:
        for i in 0..stream_len {
            if let Some(x) = x_stream.next().await {
                assert_eq!(x, xs[i]);
            } else {
                panic!("Expected x value");
            }
            if let Some(y) = y_stream.next().await {
                assert_eq!(y, ys[i]);
            } else {
                panic!("Expected y value");
            }
        }

        Ok(())
    }
}
