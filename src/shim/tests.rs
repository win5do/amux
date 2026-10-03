use super::*;
use crate::{
    model::{CreateWorkspaceRequest, RunRequest},
    store::Store,
};
use http_body_util::BodyExt;

#[tokio::test]
async fn shutdown_drains_pending_lifecycle_notifications_before_stream_eof() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::open(root.path()).unwrap();
    let workspace = store
        .create_workspace(CreateWorkspaceRequest {
            name: "dev".into(),
            ..Default::default()
        })
        .unwrap();
    let mut process = store
        .create_process(
            &workspace,
            RunRequest {
                name: "worker".into(),
                command: vec!["true".into()],
                ..Default::default()
            },
            "boot".into(),
        )
        .unwrap();
    let group = GroupShim::load(
        store.workspace_dir(&workspace.id),
        root.path().join("shim.sock"),
    )
    .unwrap();
    let response = group_events(State(group.clone())).await;
    process.state = ProcessState::Running;
    group.lifecycle.send(process.clone()).unwrap();
    process.state = ProcessState::Exited;
    group.lifecycle.send(process).unwrap();
    // Close before the response body gets its first poll: both queued events must drain.
    group.shutdown.send_replace(true);
    let body = tokio::time::timeout(Duration::from_secs(2), response.into_body().collect())
        .await
        .unwrap()
        .unwrap()
        .to_bytes();
    let states: Vec<_> = body
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Process>(line).unwrap().state)
        .collect();
    assert_eq!(states, vec![ProcessState::Running, ProcessState::Exited]);
}
