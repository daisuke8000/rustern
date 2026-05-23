//! Formatter and pipeline color setup for `run`.

use std::sync::Arc;

use crate::pipeline::ColorAssignOpts;
use crate::runtime::FormatterChoice;

use super::LineFormatter;
use super::default_renderer::DefaultLineFormatter;
use super::ext_json_renderer::ExtJsonLineFormatter;
use super::highlight::{SternHighlightLineFormatter, compile_stern_highlight_regex};
use super::json_renderer::JsonLineFormatter;
use super::raw_renderer::RawLineFormatter;

#[derive(Debug, thiserror::Error)]
pub enum RenderSetupError {
    #[error("invalid highlight/include regex: {0}")]
    HighlightRegex(#[from] regex::Error),
}

pub(crate) fn color_assign_opts(
    formatter: &FormatterChoice,
    diff_container: bool,
) -> ColorAssignOpts {
    let FormatterChoice::Default {
        pod_colors,
        container_colors,
        ..
    } = formatter
    else {
        return ColorAssignOpts {
            pod_colors: false,
            container_colors: false,
            diff_container: false,
        };
    };
    ColorAssignOpts {
        pod_colors: *pod_colors,
        container_colors: *container_colors,
        diff_container,
    }
}

pub(crate) fn line_formatter(choice: &FormatterChoice) -> Arc<dyn LineFormatter> {
    match choice {
        FormatterChoice::Default {
            timestamp_style,
            timestamp_zone,
            color_enabled,
            pod_colors,
            container_colors,
        } => Arc::new(DefaultLineFormatter {
            timestamp_style: *timestamp_style,
            timestamp_zone: *timestamp_zone,
            color_enabled: *color_enabled,
            pod_colors: *pod_colors,
            container_colors: *container_colors,
        }),
        FormatterChoice::Json => Arc::new(JsonLineFormatter),
        FormatterChoice::ExtJson { all_namespaces } => Arc::new(ExtJsonLineFormatter {
            all_namespaces: *all_namespaces,
            pretty: false,
        }),
        FormatterChoice::PpExtJson { all_namespaces } => Arc::new(ExtJsonLineFormatter {
            all_namespaces: *all_namespaces,
            pretty: true,
        }),
        FormatterChoice::Raw => Arc::new(RawLineFormatter),
    }
}

pub(crate) fn wrap_formatter_with_stern_highlight(
    formatter: &FormatterChoice,
    include: &[String],
    highlight: &[String],
    inner: Arc<dyn LineFormatter>,
) -> Result<Arc<dyn LineFormatter>, RenderSetupError> {
    let FormatterChoice::Default { .. } = formatter else {
        return Ok(inner);
    };

    Ok(match compile_stern_highlight_regex(include, highlight)? {
        Some(re) => Arc::new(SternHighlightLineFormatter::new(inner, re)),
        None => inner,
    })
}

pub(crate) fn build_line_formatter(
    formatter: &FormatterChoice,
    include: &[String],
    highlight: &[String],
) -> Result<Arc<dyn LineFormatter>, RenderSetupError> {
    wrap_formatter_with_stern_highlight(formatter, include, highlight, line_formatter(formatter))
}
