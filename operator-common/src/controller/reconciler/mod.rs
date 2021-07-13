mod error;

pub use error::*;

use async_trait::async_trait;
use core::fmt::{Debug, Formatter};

pub enum ReconcileState<I, C, D> {
    Ignore(I),
    Construct(C),
    Deconstruct(D),
}

impl<I, C, D> Debug for ReconcileState<I, C, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Ignore(..) => write!(f, "Ignore(..)"),
            Self::Construct(..) => write!(f, "Construct(..)"),
            Self::Deconstruct(..) => write!(f, "Deconstruct(..)"),
        }
    }
}

#[async_trait]
pub trait Reconciler {
    type Input;
    type Output;
    type Construct;
    type Deconstruct;

    async fn eval_state(
        &self,
        input: Self::Input,
    ) -> Result<ReconcileState<Self::Output, Self::Construct, Self::Deconstruct>, ReconcileError>;

    async fn construct(&self, c: Self::Construct) -> Result<Self::Output, ReconcileError>;
    async fn deconstruct(&self, d: Self::Deconstruct) -> Result<Self::Output, ReconcileError>;
}

pub struct ReconcileProcessor<R>(pub R)
where
    R: Reconciler;

impl<R> ReconcileProcessor<R>
where
    R: Reconciler,
{
    pub async fn reconcile(&self, input: R::Input) -> Result<R::Output, ReconcileError> {
        let state = self.0.eval_state(input).await?;
        log::debug!("Reconcile state: {:?}", state);
        match state {
            ReconcileState::Ignore(output) => Ok(output),
            ReconcileState::Construct(ctx) => self.0.construct(ctx).await,
            ReconcileState::Deconstruct(ctx) => self.0.deconstruct(ctx).await,
        }
    }
}
