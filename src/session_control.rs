use agent_client_protocol as acp;
use anyhow::Result;
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub struct SelectionOption {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ModelSelectorState {
    pub config_id: String,
    pub current_value_id: String,
    pub options: Vec<SelectionOption>,
}

#[derive(Debug, Clone)]
pub struct SessionControlState {
    pub current_permission_mode_id: Option<String>,
    pub permission_modes: Vec<SelectionOption>,
    pub model_selector: Option<ModelSelectorState>,
}

#[derive(Debug)]
pub enum SessionCommand {
    Prompt(String),
    GetControlState {
        result_tx: oneshot::Sender<Result<SessionControlState>>,
    },
    SetPermissionMode {
        mode_id: String,
        result_tx: oneshot::Sender<Result<SessionControlState>>,
    },
    SetConfigOption {
        config_id: String,
        value_id: String,
        result_tx: oneshot::Sender<Result<SessionControlState>>,
    },
}

fn flatten_select_options(
    options: &acp::SessionConfigSelectOptions,
) -> Vec<&acp::SessionConfigSelectOption> {
    match options {
        acp::SessionConfigSelectOptions::Ungrouped(values) => values.iter().collect(),
        acp::SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|group| group.options.iter())
            .collect(),
        _ => Vec::new(),
    }
}

fn find_model_selector(config_options: &[acp::SessionConfigOption]) -> Option<ModelSelectorState> {
    let model_option = config_options.iter().find(|opt| {
        let by_category = matches!(opt.category, Some(acp::SessionConfigOptionCategory::Model));
        let by_name = opt.name.to_ascii_lowercase().contains("model")
            || opt.id.0.to_ascii_lowercase().contains("model");
        by_category || by_name
    })?;

    let select = match &model_option.kind {
        acp::SessionConfigKind::Select(select) => select,
        _ => return None,
    };

    let options = flatten_select_options(&select.options)
        .into_iter()
        .map(|value| SelectionOption {
            id: value.value.0.to_string(),
            name: value.name.clone(),
        })
        .collect();

    Some(ModelSelectorState {
        config_id: model_option.id.0.to_string(),
        current_value_id: select.current_value.0.to_string(),
        options,
    })
}

pub fn build_control_state(
    mode_state: &Option<acp::SessionModeState>,
    config_options: &[acp::SessionConfigOption],
) -> SessionControlState {
    let (current_permission_mode_id, permission_modes) = match mode_state {
        Some(state) => (
            Some(state.current_mode_id.0.to_string()),
            state
                .available_modes
                .iter()
                .map(|mode| SelectionOption {
                    id: mode.id.0.to_string(),
                    name: mode.name.clone(),
                })
                .collect(),
        ),
        None => (None, Vec::new()),
    };

    SessionControlState {
        current_permission_mode_id,
        permission_modes,
        model_selector: find_model_selector(config_options),
    }
}
