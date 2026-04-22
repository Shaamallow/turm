use std::path::PathBuf;
use std::{io::BufRead, process::Command, thread, time::Duration};

use crossbeam::channel::Sender;
use regex::Regex;

use crate::app::AppMessage;
use crate::app::Job;

struct JobAcctWatcher {
    app: Sender<AppMessage>,
    interval: Duration,
    sacct_args: Vec<String>,
}

/// Filter squeue CLI args to only those compatible with sacct,
/// translating flag names where they differ.
fn sacct_compatible_args(squeue_args: &[String]) -> Vec<String> {
    squeue_args
        .iter()
        .filter_map(|arg| {
            // Translate squeue flag names to sacct equivalents
            if let Some(val) = arg.strip_prefix("--job=") {
                return Some(format!("--jobs={}", val));
            }
            if let Some(val) = arg.strip_prefix("--states=") {
                return Some(format!("--state={}", val));
            }
            // Drop flags that sacct doesn't support
            if arg == "--all"
                || arg == "--hide"
                || arg == "--me"
                || arg == "--sibling"
                || arg.starts_with("--licenses=")
                || arg.starts_with("--reservation=")
                || arg.starts_with("--step=")
                || arg.starts_with("--sort=")
            {
                return None;
            }
            // Pass through flags that work in both squeue and sacct
            Some(arg.clone())
        })
        .collect()
}

impl JobAcctWatcher {
    fn new(app: Sender<AppMessage>, interval: Duration, squeue_args: Vec<String>) -> Self {
        Self {
            app,
            interval,
            sacct_args: sacct_compatible_args(&squeue_args),
        }
    }

    fn run(&mut self) -> Self {
        let output_separator = "###turm###";
        let fields_sacct = [
            "jobid",
            "jobname",
            "state",
            "user",
            "Elapsed",
            "AllocTRES",
            "Partition",
            "NodeList",
            "SubmitLine",
            "Reason",
            "WorkDir",
            "StdOut",
            "StdErr",
        ];
        let output_format_sacct = fields_sacct.join(",");

        loop {
            let output = Command::new("sacct")
                .args(&self.sacct_args)
                .arg("--parsable2")
                .arg("--noheader")
                .arg(format!("--delimiter={}", output_separator))
                .arg("--format")
                .arg(&output_format_sacct)
                .output()
                .expect("failed to execute sacct process");

            let jobs_sacct: Vec<Job> = output
                .stdout
                .lines()
                .map(|l| l.unwrap().trim().to_string())
                .filter(|l| !l.is_empty())
                .filter_map(|l| {
                    let parts: Vec<_> = l.split(output_separator).collect();

                    if parts.len() != fields_sacct.len() {
                        return None;
                    }

                    // jobid 0,jobname 1,state 2,user 3,Elapsed 4,AllocTRES 5,Partition 6,NodeList 7,SubmitLine 8,Reason 9,WorkDir 10,StdOut 11,StdErr 12
                    let id = parts[0];
                    let name = parts[1];
                    let state = parts[2];

                    // remove the .batch and .extern jobs from sacct
                    if id.contains(".") {
                        return None;
                    }
                    // do not print running jobs, handled by squeue
                    if state == "RUNNING" {
                        return None;
                    }

                    let user = parts[3];
                    let time = parts[4];
                    let tres = parts[5];
                    let partition = parts[6];
                    let nodelist = parts[7];
                    let command = parts[8];
                    let state_compact = state.get(0..1).unwrap_or("?");
                    let reason = parts[9];
                    let node_list = parts[7];
                    let working_dir = parts[10];
                    let stdout = parts[11];
                    let stderr = parts[12];

                    // Parse array job ID from sacct jobid format: "12345_2" → master=12345, task=2
                    let (array_job_id, array_task_id) =
                        if let Some((master, task)) = id.split_once('_') {
                            (master, Some(task))
                        } else {
                            (id, None)
                        };

                    Some(Job {
                        job_id: id.to_owned(),
                        array_id: array_job_id.to_owned(),
                        array_step: array_task_id.map(|s| s.to_owned()),
                        name: name.to_owned(),
                        state: state.to_owned(),
                        state_compact: state_compact.to_owned(),
                        reason: if reason == "None" {
                            None
                        } else {
                            Some(reason.to_owned())
                        },
                        user: user.to_owned(),
                        time: time.to_owned(),
                        time_limit: String::new(),
                        start_time: String::new(),
                        tres: tres.to_owned(),
                        partition: partition.to_owned(),
                        nodelist: nodelist.to_owned(),
                        command: command.to_owned(),
                        stdout: Self::resolve_path(
                            stdout,
                            array_job_id,
                            array_task_id.unwrap_or("N/A"),
                            id,
                            node_list,
                            user,
                            name,
                            working_dir,
                        ),
                        stderr: Self::resolve_path(
                            stderr,
                            array_job_id,
                            array_task_id.unwrap_or("N/A"),
                            id,
                            node_list,
                            user,
                            name,
                            working_dir,
                        ),
                    })
                })
                .collect();

            self.app.send(AppMessage::SacctJobs(jobs_sacct)).unwrap();
            thread::sleep(self.interval);
        }
    }

    fn resolve_path(
        path: &str,
        array_master: &str,
        array_id: &str,
        id: &str,
        host: &str,
        user: &str,
        name: &str,
        working_dir: &str,
    ) -> Option<PathBuf> {
        // see https://slurm.schedmd.com/sbatch.html#SECTION_%3CB%3Efilename-pattern%3C/B%3E
        lazy_static::lazy_static! {
            static ref RE: Regex = Regex::new(r"%(%|A|a|J|j|N|n|s|t|u|x)").unwrap();
        }

        let mut path = path.to_owned();
        let slurm_no_val = "4294967294";
        let array_id = if array_id == "N/A" {
            slurm_no_val
        } else {
            array_id
        };

        if path.is_empty() {
            // never happens right now, because `squeue -O stdout` seems to always return something
            path = if array_id == slurm_no_val {
                PathBuf::from(working_dir).join("slurm-%J.out")
            } else {
                PathBuf::from(working_dir).join("slurm-%A_%a.out")
            }
            .to_str()
            .unwrap()
            .to_owned();
        };

        for cap in RE
            .captures_iter(&path.clone())
            .collect::<Vec<_>>() // TODO: this is stupid, there has to be a better way to reverse the captures...
            .iter()
            .rev()
        {
            let m = cap.get(0).unwrap();
            let replacement = match m.as_str() {
                "%%" => "%",
                "%A" => array_master,
                "%a" => array_id,
                "%J" => id,
                "%j" => id,
                "%N" => host.split(',').next().unwrap_or(host),
                "%n" => "0",
                "%s" => "batch",
                "%t" => "0",
                "%u" => user,
                "%x" => name,
                _ => unreachable!(),
            };

            path.replace_range(m.range(), replacement);
        }

        Some(PathBuf::from(working_dir).join(path)) // works even if `path` is absolute
    }
}

pub struct JobAcctWatcherHandle {}

impl JobAcctWatcherHandle {
    pub fn new(app: Sender<AppMessage>, interval: Duration, squeue_args: Vec<String>) -> Self {
        let mut actor = JobAcctWatcher::new(app, interval, squeue_args);
        thread::spawn(move || actor.run());

        Self {}
    }
}
