use crate::{Event, EventSender, SenderResult};
use async_trait::async_trait;
use drogue_cloud_database_common::{
    error::ServiceError,
    models::outbox::{OutboxAccessor, OutboxEntry},
};

impl From<Event> for OutboxEntry {
    fn from(e: Event) -> Self {
        match e {
            Event::Application {
                instance,
                application,
                uid,
                path,
                generation,
            } => OutboxEntry {
                instance,
                app: application,
                device: None,
                uid,
                path,
                generation,
            },
            Event::Device {
                instance,
                application,
                device,
                uid,
                path,
                generation,
            } => OutboxEntry {
                instance,
                app: application,
                device: Some(device),
                uid,
                path,
                generation,
            },
        }
    }
}

impl From<OutboxEntry> for Event {
    fn from(entry: OutboxEntry) -> Self {
        if let Some(device) = entry.device {
            Event::Device {
                instance: entry.instance,
                application: entry.app,
                device,
                path: entry.path,
                generation: entry.generation,
                uid: entry.uid,
            }
        } else {
            Event::Application {
                instance: entry.instance,
                application: entry.app,
                path: entry.path,
                generation: entry.generation,
                uid: entry.uid,
            }
        }
    }
}

#[async_trait]
impl<A> EventSender for A
where
    A: OutboxAccessor + Send + Sync,
{
    type Error = ServiceError;

    async fn notify<I>(&self, events: I) -> SenderResult<(), Self::Error>
    where
        I: IntoIterator<Item = Event> + Sync + Send,
        I::IntoIter: Sync + Send,
    {
        for e in events.into_iter() {
            self.create(e.into()).await?;
        }

        Ok(())
    }
}
