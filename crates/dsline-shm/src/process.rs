//! Writer process identity for crash recovery (roadmap §6).
//!
//! A writer records `pid + start_time` in every slot header it marks
//! WRITING. Before reclaiming a crashed writer's leftover slot, the
//! reclaimer must verify that the identity is no longer live:
//!
//! ```text
//! process identity = pid + start_time
//! ```
//!
//! A slot may be reclaimed only when the identity is provably gone (the PID
//! does not exist, or the PID exists with a different start time — PID
//! reuse), or when the owner explicitly runs cleanup. When the platform
//! cannot decide reliably, the answer is `Unknown` and the caller must NOT
//! reclaim (the roadmap's conservative default: never risk hurting a live
//! writer).
//!
//! Start-time representations are platform-local (Windows FILETIME 100 ns
//! ticks, Linux clock ticks since boot) and are only ever compared against
//! values recorded on the same machine and platform family.

/// A writer's process identity: OS pid plus a platform-local process start
/// timestamp. `pid == 0` is never a valid identity (unset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub start_time: u64,
}

/// Liveness of a recorded writer identity, from the reclaimer's viewpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessLiveness {
    /// A process with this exact identity is running — never reclaim.
    Alive,
    /// The identity is provably gone: the PID does not exist, or the PID is
    /// alive but was started later (the old process exited and the PID was
    /// reused). Safe to reclaim.
    Gone,
    /// The platform cannot decide (unsupported platform, insufficient
    /// permission, or a transient error). Never reclaim.
    Unknown,
}

/// The calling process's identity.
pub fn current_process_identity() -> ProcessIdentity {
    ProcessIdentity {
        pid: std::process::id(),
        start_time: current_process_start_time(),
    }
}

/// Classify a recorded writer identity. This is the only decision the
/// reclaim protocol may base a reclaim on.
pub fn process_liveness(identity: ProcessIdentity) -> ProcessLiveness {
    if identity.pid == 0 {
        // Unset (legacy or zero-initialized header): no writer to verify.
        return ProcessLiveness::Unknown;
    }
    match process_start_time(identity.pid) {
        // PID does not exist: the identity is gone regardless of the
        // recorded start time.
        Ok(None) => ProcessLiveness::Gone,
        // PID exists: the identity survives only if the start time matches
        // exactly (a mismatch means the original process exited and the PID
        // was reused by a younger process).
        Ok(Some(start_time)) => {
            if start_time == identity.start_time {
                ProcessLiveness::Alive
            } else {
                ProcessLiveness::Gone
            }
        }
        Err(_) => ProcessLiveness::Unknown,
    }
}

#[cfg(windows)]
mod imp {
    use std::os::raw::c_void;

    #[allow(non_camel_case_types)]
    type HANDLE = *mut c_void;

    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const ERROR_INVALID_PARAMETER: u32 = 87;
    const ERROR_ACCESS_DENIED: u32 = 5;

    #[link(name = "kernel32")]
    extern "system" {
        pub fn OpenProcess(
            dw_desired_access: u32,
            b_inherit_handle: i32,
            dw_process_id: u32,
        ) -> HANDLE;
        pub fn GetProcessTimes(
            h_process: HANDLE,
            lp_creation_time: *mut i64,
            lp_exit_time: *mut i64,
            lp_kernel_time: *mut i64,
            lp_user_time: *mut i64,
        ) -> i32;
        pub fn GetCurrentProcess() -> HANDLE;
        pub fn CloseHandle(h_object: HANDLE) -> i32;
        pub fn GetLastError() -> u32;
    }

    /// Creation time as a 100 ns FILETIME value (Windows epoch 1601).
    fn creation_time(handle: HANDLE) -> Result<u64, ()> {
        let mut creation = 0i64;
        let mut exit = 0i64;
        let mut kernel = 0i64;
        let mut user = 0i64;
        let ok =
            unsafe { GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) };
        if ok == 0 {
            return Err(());
        }
        Ok(creation as u64)
    }

    pub fn current_process_start_time() -> u64 {
        // SAFETY: the pseudo-handle needs no lifetime management and the
        // out-pointers point at locals that outlive the call.
        creation_time(unsafe { GetCurrentProcess() }).unwrap_or(0)
    }

    /// `Ok(None)` = no such process; `Ok(Some(t))` = creation FILETIME;
    /// `Err` = cannot decide (access denied or a transient error).
    pub fn process_start_time(pid: u32) -> Result<Option<u64>, ()> {
        // SAFETY: `pid` is caller-supplied; the returned handle (if any) is
        // closed on every path below.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            let err = unsafe { GetLastError() };
            return match err {
                ERROR_INVALID_PARAMETER => Ok(None),
                // A protected process exists but refuses inspection. The
                // recorded identity may still be alive: do not reclaim.
                ERROR_ACCESS_DENIED => Err(()),
                _ => Err(()),
            };
        }
        let result = creation_time(handle);
        // SAFETY: the handle came from `OpenProcess` and is closed exactly
        // once here.
        unsafe { CloseHandle(handle) };
        result.map(Some)
    }
}

#[cfg(target_os = "linux")]
mod imp {

    pub fn current_process_start_time() -> u64 {
        read_start_time(std::path::Path::new("/proc/self"))
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    /// `Ok(None)` = no such process; `Ok(Some(t))` = starttime in clock
    /// ticks since boot; `Err` = cannot decide.
    pub fn process_start_time(pid: u32) -> Result<Option<u64>, ()> {
        read_start_time(std::path::Path::new(&format!("/proc/{pid}")))
    }

    /// Reads field 22 (`starttime`) of `/proc/<pid>/stat`.
    ///
    /// The `comm` field (field 2) is parenthesized and may contain spaces
    /// and even parentheses, so the fields are parsed after the LAST `)` —
    /// splitting on whitespace naively mis-parses processes like
    /// `(a b (c))`.
    fn read_start_time(proc_path: &std::path::Path) -> Result<Option<u64>, ()> {
        let stat = match std::fs::read_to_string(proc_path.join("stat")) {
            Ok(stat) => stat,
            Err(e) => {
                return match e.kind() {
                    std::io::ErrorKind::NotFound => match std::fs::metadata(proc_path) {
                        // The /proc entry exists but stat is unreadable:
                        // cannot decide.
                        Ok(_) => Err(()),
                        // The process is gone.
                        Err(_) => Ok(None),
                    },
                    _ => Err(()),
                };
            }
        };
        let after_comm = match stat.rfind(')') {
            Some(idx) => &stat[idx + 1..],
            None => return Err(()),
        };
        // After `comm)` the next token is field 3 (state); field 22
        // (`starttime`) is the 20th token from here.
        let starttime = after_comm
            .split_whitespace()
            .nth(19)
            .ok_or(())?
            .parse::<u64>()
            .map_err(|_| ())?;
        Ok(Some(starttime))
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
mod imp {

    pub fn current_process_start_time() -> u64 {
        0
    }

    /// Platforms without a reliable process-identity probe never answer:
    /// the roadmap's conservative default (no auto-reclaim).
    pub fn process_start_time(_pid: u32) -> Result<Option<u64>, ()> {
        Err(())
    }
}

use imp::{current_process_start_time, process_start_time};

#[cfg(test)]
mod tests {
    use super::*;

    fn spawn_and_reap() -> u32 {
        // Spawn a child that exits immediately, then reap it: its PID is
        // afterwards free (and, given PID reuse delays, practically never
        // recycled within the test).
        #[cfg(windows)]
        let mut child = std::process::Command::new("cmd")
            .args(["/c", "exit"])
            .spawn()
            .expect("spawn probe child");
        #[cfg(not(windows))]
        let mut child = std::process::Command::new("true")
            .spawn()
            .expect("spawn probe child");
        let pid = child.id();
        child.wait().expect("reap probe child");
        pid
    }

    #[test]
    fn current_process_identity_is_alive() {
        let identity = current_process_identity();
        assert_ne!(identity.pid, 0);
        assert_eq!(process_liveness(identity), ProcessLiveness::Alive);
    }

    #[test]
    fn current_identity_survives_a_roundtrip() {
        let identity = current_process_identity();
        assert_eq!(
            process_start_time(identity.pid),
            Ok(Some(identity.start_time))
        );
    }

    #[test]
    fn reaped_process_identity_is_gone() {
        let pid = spawn_and_reap();
        // The exact start_time does not matter: the PID no longer exists.
        let identity = ProcessIdentity { pid, start_time: 0 };
        assert_eq!(process_liveness(identity), ProcessLiveness::Gone);
    }

    #[test]
    fn pid_reuse_is_detected_as_gone() {
        let pid = spawn_and_reap();
        // A "reused" PID that exists with a different start time: simulate
        // by checking the current process under a WRONG start time.
        let identity = ProcessIdentity {
            pid: std::process::id(),
            start_time: current_process_start_time().wrapping_add(1),
        };
        assert_eq!(process_liveness(identity), ProcessLiveness::Gone);
        // ...and the probe child's PID must not accidentally alias ours.
        assert_ne!(pid, std::process::id());
    }

    #[test]
    fn unset_identity_is_unknown() {
        let identity = ProcessIdentity {
            pid: 0,
            start_time: 12345,
        };
        assert_eq!(process_liveness(identity), ProcessLiveness::Unknown);
    }

    #[test]
    fn improbable_pid_is_gone_not_unknown() {
        // A PID beyond the platform's process-id range must classify as
        // Gone (no such process), not Unknown. Windows pids are < 2^22ish
        // in practice but the API accepts u32; a huge value cannot exist.
        let identity = ProcessIdentity {
            pid: u32::MAX,
            start_time: 0,
        };
        assert_eq!(process_liveness(identity), ProcessLiveness::Gone);
    }
}
