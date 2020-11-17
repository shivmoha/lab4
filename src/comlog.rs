use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::process::exit;
use std::sync::{Arc, Mutex};

use commitlog::{CommitLog, LogOptions, ReadLimit};
use commitlog::AppendError::MessageSizeExceeded;
use commitlog::message::{MessageBuf, MessageSet};
use commitlog::reader::{LogSliceReader, MessageBufReader};

use message;
use message::{MessageType, ProtocolMessage};
use oplog::OpLog;

pub struct CLog {
    log: CommitLog,
}

impl Debug for CLog {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Debug")
    }
}

impl CLog {
    pub fn new(fpath: String) -> CLog {
        trace!("Removed existing directory {}", fpath.clone());
        std::fs::remove_dir_all(fpath.clone());
        let opts = LogOptions::new(fpath);
        let mut log = CommitLog::new(opts).unwrap();
        CLog {
            log: log
        }
    }

    pub fn from_file(fpath: String) -> MessageBuf {
        let opts = LogOptions::new(fpath);
        let mut log = CommitLog::new(opts).unwrap();
        let messages = log.read(0, ReadLimit::max_bytes(1024 * 10000)).unwrap();
        return messages;
    }

    pub fn append(&mut self, t: message::MessageType, tid: i32, sender: String, op: i32) {
        let pm = message::ProtocolMessage::generate(t, tid, sender, op);
        self.log.append_msg(ProtocolMessage::to_string(&pm)).unwrap();
    }

    pub fn read_all_message(&mut self) -> MessageBuf {
        let messages = self.log.read(0, ReadLimit::max_bytes(1024 * 10000)).unwrap();
        return messages;
    }

    pub fn read(&mut self, offset: i32) -> message::ProtocolMessage {
        let messages = self.log.read(offset as u64, ReadLimit::default()).unwrap();
        for msg in messages.iter() {
            println!("{} - {}", msg.offset(), String::from_utf8_lossy(msg.payload()));
            break;
        }
        let mut result = ProtocolMessage {
            mtype: MessageType::ClientRequest,
            uid: 0,
            txid: 0,
            senderid: "".to_string(),
            opid: 0,
        };
        return result;
    }
}