use std::mem;
use std::ops::{Deref, DerefMut};
use strum_macros::EnumDiscriminants;
use unsync::spsc::Receiver as SpscReceiver;
use unsync::spsc::Sender as SpscSender;

/// Internal Communication for e.g., structs that spawn tasks and need to communicate between them
/// The idea is that the receiver is taken by the spawned task, and the sender is kept.
/// This is an abstraction for a pattern that is used in multiple places.
#[derive(EnumDiscriminants)]
#[strum_discriminants(name(InternalCommState))]
pub enum InternalComm<T> {
    Setup(InternalCommSetup<T>),
    Sender(InternalCommSender<T>),
    Receiver(InternalCommReceiver<T>),
}

pub struct InternalCommSetup<T> {
    sender: SpscSender<T>,
    receiver: SpscReceiver<T>,
}

pub struct InternalCommSender<T> {
    sender: SpscSender<T>,
}

pub struct InternalCommReceiver<T> {
    receiver: SpscReceiver<T>,
}

impl<T> InternalComm<T> {
    pub fn new(buffer: usize) -> Self {
        let (tx, rx) = unsync::spsc::channel::<T>(buffer);
        let state = Self::Setup(InternalCommSetup {
            sender: tx,
            receiver: rx,
        });
        state
    }

    /// Get current state
    pub fn state(&self) -> InternalCommState {
        match &self {
            InternalComm::Setup(_) => InternalCommState::Setup,
            InternalComm::Sender(_) => InternalCommState::Sender,
            InternalComm::Receiver(_) => InternalCommState::Receiver,
        }
    }

    /// Split from Setup state into two objects: (Sender, Receiver)
    #[allow(dead_code)]
    pub fn split(self) -> anyhow::Result<(Self, Self)> {
        match self {
            InternalComm::Setup(setup) => {
                let sender = InternalCommSender {
                    sender: setup.sender,
                };
                let receiver = InternalCommReceiver {
                    receiver: setup.receiver,
                };
                Ok((Self::Sender(sender), Self::Receiver(receiver)))
            }
            _ => Err(anyhow::anyhow!(
                "Cannot split InternalComm that is not in Setup state"
            )),
        }
    }

    /// Change self from Setup state into Sender and detach Receiver
    pub fn split_receiver(&mut self) -> anyhow::Result<Self> {
        match self {
            InternalComm::Setup(_) => {
                let old_state = mem::replace(&mut *self, InternalComm::new(1));
                let setup = match old_state {
                    InternalComm::Setup(s) => s,
                    _ => unreachable!(),
                };
                *self = InternalComm::Sender(InternalCommSender {
                    sender: setup.sender,
                });
                Ok(InternalComm::Receiver(InternalCommReceiver {
                    receiver: setup.receiver,
                }))
            }
            _ => Err(anyhow::anyhow!(
                "Cannot split InternalComm that is not in Setup state"
            )),
        }
    }

    /// Useful when wanting to use the Sender directly wanting to deal with wrapper
    #[allow(dead_code)]
    pub fn take_sender(self) -> anyhow::Result<InternalCommSender<T>> {
        match self {
            InternalComm::Sender(sender) => Ok(sender),
            _ => Err(anyhow::anyhow!(
                "Cannot take sender from InternalComm that is not in Sender state"
            )),
        }
    }

    /// Useful when wanting to use the Receiver directly wanting to deal with wrapper
    pub fn take_receiver(self) -> anyhow::Result<InternalCommReceiver<T>> {
        match self {
            InternalComm::Receiver(receiver) => Ok(receiver),
            _ => Err(anyhow::anyhow!(
                "Cannot take sender from InternalComm that is not in Receiver state"
            )),
        }
    }
}

impl<T> Deref for InternalCommSender<T> {
    type Target = SpscSender<T>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<T> DerefMut for InternalCommSender<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}

impl<T> Deref for InternalCommReceiver<T> {
    type Target = SpscReceiver<T>;

    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl<T> DerefMut for InternalCommReceiver<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}
