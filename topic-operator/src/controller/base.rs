use anyhow::{anyhow, Error};
use async_trait::async_trait;
use drogue_client::{error::ClientError, registry};
use drogue_cloud_registry_events::Event;
use std::ops::Deref;
use std::time::Duration;

pub struct WorkQueue<K> {
    queue: Vec<K>,
}

impl<K> Default for WorkQueue<K> {
    fn default() -> Self {
        WorkQueue { queue: vec![] }
    }
}

impl<K> WorkQueue<K> {
    pub async fn add(&mut self, key: K, after: Duration) -> Result<(), ()> {
        // FIXME: implement
        self.queue.push(key);
        Ok(())
    }
}

pub struct EventSource<K, P>
where
    P: EventProcessor<Key = K>,
{
    processor: P,
}

impl<K, P> EventSource<K, P>
where
    P: EventProcessor<Key = K>,
{
    pub fn new(processor: P) -> Self {
        Self { processor }
    }

    /// Handle the event.
    pub async fn handle(&mut self, event: Event) -> Result<(), ()> {
        match self.processor.is_relevant(&event) {
            Some(key) => self.processor.process(key).await?,
            None => {}
        }
        Ok(())
    }
}

#[async_trait]
pub trait EventProcessor {
    type Key;

    /// Translate into key, or nothing.
    fn is_relevant(&self, event: &Event) -> Option<Self::Key>;

    /// Process the event
    async fn process(&mut self, key: Self::Key) -> Result<(), ()>;
}

pub struct FnEventProcessor<'p, K, O, F>
where
    K: Send + Sync,
    O: ControllerOperation<Key = K> + Send + Sync,
    F: Fn(&Event) -> Option<K>,
{
    base_controller: &'p mut BaseController<K, O>,
    f: F,
}

impl<'p, K, O, F> FnEventProcessor<'p, K, O, F>
where
    K: Send + Sync,
    O: ControllerOperation<Key = K> + Send + Sync,
    F: Fn(&Event) -> Option<K>,
{
    pub fn new(base_controller: &'p mut BaseController<K, O>, f: F) -> Self {
        Self { base_controller, f }
    }
}

#[async_trait]
impl<'p, K, O, F> EventProcessor for FnEventProcessor<'p, K, O, F>
where
    K: Send + Sync,
    O: ControllerOperation<Key = K> + Send + Sync,
    F: Fn(&Event) -> Option<K> + Send,
{
    type Key = K;

    fn is_relevant(&self, event: &Event) -> Option<Self::Key> {
        (self.f)(event)
    }

    async fn process(&mut self, key: Self::Key) -> Result<(), ()> {
        self.base_controller.process(key).await
    }
}

pub struct BaseController<K, O>
where
    O: ControllerOperation<Key = K> + Send + Sync,
{
    operation: O,
    queue: WorkQueue<K>,
}

impl<K, O> BaseController<K, O>
where
    O: ControllerOperation<Key = K> + Send + Sync,
{
    const MAX_RETRIES: usize = 10;

    pub fn new(operation: O) -> Self {
        Self {
            operation,
            queue: Default::default(),
        }
    }

    pub async fn process(&mut self, key: K) -> Result<(), ()> {
        let mut retries: usize = 0;
        loop {
            match self.operation.process(&key).await? {
                OperationOutcome::Complete => break Ok(()),
                OperationOutcome::RetryNow => {
                    retries += 1;
                    if retries > Self::MAX_RETRIES {
                        self.queue.add(key, Duration::ZERO).await?;
                        break Ok(());
                    } else {
                        continue;
                    }
                }
                OperationOutcome::RetryLater(delay) => {
                    self.queue.add(key, delay).await?;
                    break Ok(());
                }
            }
        }
    }
}

pub enum OperationOutcome {
    Complete,
    RetryNow,
    RetryLater(Duration),
}

impl OperationOutcome {
    pub fn retry(delay: Option<Duration>) -> Self {
        match delay {
            Some(delay) if !delay.is_zero() => Self::RetryLater(delay),
            _ => Self::RetryNow,
        }
    }
}

#[async_trait]
pub trait ResourceOperations {
    type Key: Send + Sync;
    type Resource: Clone + Send + Sync;

    /// Get the resource.
    async fn get(
        &self,
        key: &Self::Key,
    ) -> Result<Option<Self::Resource>, ClientError<reqwest::Error>>;

    /// Update the resource if it did change.
    async fn update_if(
        &self,
        original: &Self::Resource,
        current: &Self::Resource,
    ) -> Result<OperationOutcome, ()>;
}

#[async_trait]
pub trait ControllerOperation: ResourceOperations {
    async fn process_resource(
        &self,
        application: Self::Resource,
    ) -> Result<ProcessOutcome<Self::Resource>, ()>;

    /// Process the key, any error returned is a fatal error,
    async fn process(&self, key: &Self::Key) -> Result<OperationOutcome, ()> {
        // read the resource ...
        match self.get(key).await {
            // ... and process it
            Ok(Some(resource)) => match self.process_resource(resource.clone()).await? {
                // ... completed -> store and return(done)
                ProcessOutcome::Complete(outcome) => {
                    self.update_if(&resource, &outcome).await?;
                    Ok(OperationOutcome::Complete)
                }
                // ... need to re-try -> store and return(retry)
                ProcessOutcome::Retry(outcome, delay) => {
                    self.update_if(&resource, &outcome).await?;
                    Ok(OperationOutcome::retry(delay))
                }
            },
            // ... nothing found, we are done.
            Ok(None) => {
                // resource is gone, we have finalizers to guard against this
                Ok(OperationOutcome::Complete)
            }
            // ... error -> retry
            Err(_) => Ok(OperationOutcome::RetryNow),
        }
    }
}

#[async_trait]
impl<S> ResourceOperations for S
where
    S: Deref<Target = registry::v1::Client> + Send + Sync,
{
    type Key = String;
    type Resource = registry::v1::Application;

    async fn get(
        &self,
        key: &Self::Key,
    ) -> Result<Option<registry::v1::Application>, ClientError<reqwest::Error>> {
        self.get_app(&key, Default::default()).await
    }

    async fn update_if(
        &self,
        original: &registry::v1::Application,
        current: &registry::v1::Application,
    ) -> Result<OperationOutcome, ()> {
        if original != current {
            match self.update_app(current, Default::default()).await {
                Ok(_) => Ok(OperationOutcome::Complete),
                Err(err) => match err {
                    ClientError::Syntax(_) => Ok(OperationOutcome::Complete),
                    _ => Ok(OperationOutcome::RetryNow),
                },
            }
        } else {
            Ok(OperationOutcome::Complete)
        }
    }
}

pub enum ProcessOutcome<T> {
    Complete(T),
    Retry(T, Option<Duration>),
}

pub struct ApplicationController {
    client: registry::v1::Client,
}

impl ApplicationController {
    pub fn new(client: registry::v1::Client) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ControllerOperation for ApplicationController {
    async fn process_resource(
        &self,
        application: Self::Resource,
    ) -> Result<ProcessOutcome<Self::Resource>, ()> {
        todo!()
    }
}

impl Deref for ApplicationController {
    type Target = registry::v1::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
