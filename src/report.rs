//! User-facing error rendering via `miette`.

use std::process;

pub(crate) fn install_hook() {
    let _ = miette::set_hook(Box::new(|_| {
        Box::new(miette::MietteHandlerOpts::new().build())
    }));
}

pub(crate) fn fail(report: miette::Report) -> ! {
    fail_with_code(report, 1);
}

pub(crate) fn fail_msg(msg: impl Into<String>) -> ! {
    fail(miette::Report::msg(msg.into()));
}

pub(crate) fn fail_run(err: rustern_core::RunError) -> ! {
    fail(miette::Report::msg(format!("{err:#}")));
}

pub(crate) fn fail_with_code(report: miette::Report, code: i32) -> ! {
    eprintln!("{report}");
    process::exit(code);
}
