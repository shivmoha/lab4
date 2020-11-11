extern crate clap;
//extern crate commitlog;
extern crate ctrlc;
#[macro_use]
extern crate log;
extern crate serde_json;
extern crate stderrlog;

use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::{JoinHandle, sleep};
use std::time::Duration;

//use commitlog::*;

use client::Client;
use coordinator::Coordinator;
use message::MessageType::ClientRequest;
use participant::Participant;

pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
pub mod comlog;

///
/// register_clients()
/// 
/// The coordinator needs to know about all clients. 
/// This function should create clients and use some communication 
/// primitive to ensure the coordinator and clients are aware of 
/// each other and able to exchange messages. Starting threads to run the
/// client protocol should be deferred until after all the communication 
/// structures are created. 
/// 
/// HINT: you probably want to look at rust's mpsc::channel or crossbeam 
///       channels to set up communication. Communication in 2PC 
///       is duplex!
/// 
/// HINT: read the logpathbase documentation carefully.
/// 
/// <params>
///     coordinator: the coordinator!
///     n_clients: number of clients to create and register
///     logpathbase: each participant, client, and the coordinator 
///         needs to maintain its own operation and commit log. 
///         The project checker assumes a specific directory structure 
///         for files backing these logs. Concretely, participant log files 
///         will be expected to be produced in:
///            logpathbase/client_<num>.log
///     running: atomic bool indicating whether the simulation is still running
///
fn register_clients(
    coordinator: &mut Coordinator,
    n_clients: i32,
    log_path_base: &String,
    running: &Arc<AtomicBool>) -> Vec<Client> {
    let mut clients = vec![];
    // register clients with coordinator (set up communication channels and sync objects)
    // add client to the vector and return the vector.
    for c in 0..n_clients {
        let clientName = format!("{}{}", "Client_", c);
        let (clientSend, clientReceive) = coordinator.client_join(&clientName);
        let clientLogPath = format!("{}/{}", log_path_base, clientName);
        trace!("Registering client : {} Logs at : {}", c, clientLogPath);
        clients.push(Client::new(c, String::new(), clientSend, clientReceive, running.clone()));
    }
    return clients;
}

/// 
/// register_participants()
/// 
/// The coordinator needs to know about all participants. 
/// This function should create participants and use some communication 
/// primitive to ensure the coordinator and participants are aware of 
/// each other and able to exchange messages. Starting threads to run the
/// participant protocol should be deferred until after all the communication 
/// structures are created. 
/// 
/// HINT: you probably want to look at rust's mpsc::channel or crossbeam 
///       channels to set up communication. Note that communication in 2PC 
///       is duplex!
/// 
/// HINT: read the logpathbase documentation carefully.
/// 
/// <params>
///     coordinator: the coordinator!
///     n_participants: number of participants to create an register
///     logpathbase: each participant, client, and the coordinator 
///         needs to maintain its own operation and commit log. 
///         The project checker assumes a specific directory structure 
///         for files backing these logs. Concretely, participant log files 
///         will be expected to be produced in:
///            logpathbase/participant_<num>.log
///     running: atomic bool indicating whether the simulation is still running
///     success_prob: [0.0..1.0] probability that operations or sends succeed.
///
fn register_participants(
    coordinator: &mut Coordinator,
    n_participants: i32,
    log_path_base: &String,
    running: &Arc<AtomicBool>,
    success_prob_ops: f64,
    success_prob_msg: f64) -> Vec<Participant> {
    let mut participants = vec![];
    // register participants with coordinator (set up communication channels and sync objects)
    // add client to the vector and return the vector.
    for i in 0..n_participants {
        trace!("Participant_{} joining", i);
        let participantName = format!("{}{}", "participant_", i);
        let (participantSend, participantReceive) = coordinator.participant_join(&participantName);
        let participantPath = format!("{}/{}.log", log_path_base, participantName);
        trace!("Registering participant : {} Logs at : {}", i, participantPath);
        participants.push(Participant::new(i, String::new(), participantSend, participantReceive, participantPath, running.clone(),
                                           success_prob_ops, success_prob_msg));
    }
    return participants;
}

///
/// launch_clients()
/// 
/// create a thread per client to run the client
/// part of the 2PC protocol. Somewhere in each of the threads created
/// here, there should be a call to Client::protocol(...). Telling the client
/// how many requests to send is probably a good idea. :-)
/// 
/// <params>
/// participants: a vector of Participant structs
/// handles: (optional depending on design) -- a mutable vector 
///    to return wait handles to the caller
///
fn launch_clients(
    clients: Vec<Client>,
    n_requests: i32,
    handles: &mut Vec<JoinHandle<()>>) -> &mut Vec<JoinHandle<()>> {
    // do something to create threads for client 'processes'
    // the mutable handles parameter allows you to return 
    // more than one wait handle to the caller to join on.
    for mut client in clients {
        let handle = thread::spawn(move || {
            // debug!("in thread ");
            // thread::sleep(Duration::from_millis(4000));
            client.protocol(n_requests);
        });
        handles.push(handle);
    }
    return handles;
}

///
/// launch_participants()
/// 
/// create a thread per participant to run the participant 
/// part of the 2PC protocol. Somewhere in each of the threads created
/// here, there should be a call to Participant::participate(...).
/// 
/// <params>
/// participants: a vector of Participant structs
/// handles: (optional depending on design) -- a mutable vector 
///    to return wait handles to the caller
///
fn launch_participants(
    participants: Vec<Participant>,
    handles: &mut Vec<JoinHandle<()>>) -> &mut Vec<JoinHandle<()>> {
    for mut participant in participants {
        let handle = thread::spawn(move || {
            // debug!("in thread ");
            // thread::sleep(Duration::from_millis(4000));
            participant.protocol();
        });
        handles.push(handle);
    }
    return handles;
    // do something to create threads for participant 'processes'
    // the mutable handles parameter allows you to return 
    // more than one wait handle to the caller to join on. 
}

/// 
/// run()
/// opts: an options structure describing mode and parameters
/// 
/// 0. install a signal handler that manages a global atomic boolean flag
/// 1. creates a new coordinator
/// 2. creates new clients and registers them with the coordinator
/// 3. creates new participants and registers them with coordinator
/// 4. launches participants in their own threads
/// 5. launches clients in their own threads
/// 6. creates a thread to run the coordinator protocol
/// 
fn run(opts: &tpcoptions::TPCOptions) {

    // vector for wait handles, allowing us to 
    // wait for client, participant, and coordinator 
    // threads to join.
    let mut participantHandles: Vec<JoinHandle<()>> = vec![];
    let mut clientHandles: Vec<JoinHandle<()>> = vec![];


    // create an atomic bool object and a signal handler
    // that sets it. this allows us to inform clients and 
    // participants that we are exiting the simulation 
    // by pressing "control-C", which will set the running 
    // flag to false. 
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!(" CTRL-C!");
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting signal handler!");

    // create a coordinator, create and register clients and participants
    // launch threads for all, and wait on handles. 
    let cpath = format!("{}/{}", opts.logpath, "coordinator.log");
    debug!("{}", cpath);
    let mut coordinator: Coordinator;
    let clients: Vec<Client>;
    let participants: Vec<Participant>;

    coordinator = Coordinator::new(cpath, running.clone(), opts.success_probability_ops);
    clients = register_clients(&mut coordinator, opts.num_clients, &opts.logpath, &running);
    participants = register_participants(&mut coordinator, opts.num_participants, &opts.logpath, &running,
                                         opts.success_probability_ops, opts.success_probability_msg);


    let coordinatorHandle = thread::spawn(move || {
        coordinator.protocol();
    });
    launch_clients(clients, opts.num_requests, &mut clientHandles);
    launch_participants(participants, &mut participantHandles);


    // wait for clients, participants, and coordinator here...
    for participant in participantHandles {
        participant.join().expect("oops! the participant thread panicked");
    }
    coordinatorHandle.join().expect("oops! the coordinator thread panicked");
    for client in clientHandles {
        client.join().expect("oops! the client thread panicked");
    }
}

///
/// main()
/// 
fn main() {
    let opts = tpcoptions::TPCOptions::new();
    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .timestamp(stderrlog::Timestamp::Millisecond)
        .verbosity(opts.verbosity)
        .init()
        .unwrap();

    match opts.mode.as_ref() {
        "run" => run(&opts),
        "check" => checker::check_last_run(opts.num_clients,
                                           opts.num_requests,
                                           opts.num_participants,
                                           &opts.logpath.to_string()),
        _ => panic!("unknown mode"),
    }
}
