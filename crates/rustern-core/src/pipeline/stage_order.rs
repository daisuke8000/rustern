//! Explicit pipeline stage ordering for exit triggers vs display filters.
//!
//! rustern-plus exit triggers must observe events **before** `-i`/`-e` hide them when
//! `filter_on=original`. Level-based exit defers include/exclude until after classify (and jq)
//! so triggers see classified levels on the raw line shape.

use super::FilterOn;

/// Resolved placement of include/exclude and early message exit relative to other stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineStageOrder {
    /// Run `--exit-on` message matching on the stream before container/include filters.
    pub exit_on_message_before_filters: bool,
    /// Apply `-i`/`-e` before container filter (original path without level exit).
    pub include_before_container: bool,
    /// Apply `-i`/`-e` after jq (transformed path, or original + `--exit-on-level`).
    pub include_after_transform: bool,
}

impl PipelineStageOrder {
    /// Derive stage order from CLI-equivalent filter and exit options.
    pub fn resolve(
        filter_on: FilterOn,
        has_exit_on_message: bool,
        has_exit_on_level: bool,
    ) -> Self {
        match filter_on {
            FilterOn::Original => {
                let defer_include = has_exit_on_level;
                Self {
                    exit_on_message_before_filters: has_exit_on_message,
                    include_before_container: !defer_include,
                    include_after_transform: defer_include,
                }
            }
            FilterOn::Transformed => Self {
                exit_on_message_before_filters: false,
                include_before_container: false,
                include_after_transform: true,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_without_exit_runs_include_before_container() {
        let order = PipelineStageOrder::resolve(FilterOn::Original, false, false);
        assert!(order.include_before_container);
        assert!(!order.include_after_transform);
        assert!(!order.exit_on_message_before_filters);
    }

    #[test]
    fn original_exit_on_message_runs_before_filters() {
        let order = PipelineStageOrder::resolve(FilterOn::Original, true, false);
        assert!(order.exit_on_message_before_filters);
        assert!(order.include_before_container);
        assert!(!order.include_after_transform);
    }

    #[test]
    fn original_exit_on_level_defers_include_until_after_transform() {
        let order = PipelineStageOrder::resolve(FilterOn::Original, false, true);
        assert!(!order.include_before_container);
        assert!(order.include_after_transform);
    }

    #[test]
    fn original_both_exit_triggers_defer_include() {
        let order = PipelineStageOrder::resolve(FilterOn::Original, true, true);
        assert!(order.exit_on_message_before_filters);
        assert!(!order.include_before_container);
        assert!(order.include_after_transform);
    }

    #[test]
    fn transformed_always_defers_include_and_places_message_exit_after_container() {
        let order = PipelineStageOrder::resolve(FilterOn::Transformed, true, true);
        assert!(!order.exit_on_message_before_filters);
        assert!(!order.include_before_container);
        assert!(order.include_after_transform);
    }
}
