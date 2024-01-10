#![allow(unused_variables, dead_code, unused_mut)]

use std::io::{Read, Seek};
use std::mem::size_of;
use std::sync::mpsc::{channel, sync_channel, Receiver, Sender, SyncSender};
// use crossbeam_channel

use std::{
    collections::HashMap,
    fs::{self, File},
    io::BufRead,
};
fn main() {
    println!("Hello, world!");
    // let file = fs::File::open("/home/manya/dev/kumarmo2/1brc/50-measurements.txt").unwrap();
    let file = fs::File::open("/home/manya/dev/kumarmo2/1brc/measurements.txt").unwrap();
    // most_basic_implementation(file);
    // most_basic_implementation_v2(file);
    // most_basic_implementation_v2_more_heap_allocation(file);
    // single_producer_single_consumer(file);
    // single_producer_single_consumer_crossbeam(file);
    some_single_thread_enhancements(file);
}

#[derive(Debug, Default)]
struct StationProps {
    min: f32,
    max: f32,
    count: u32,
    sum: f32,
}

fn single_producer_single_consumer_crossbeam(file: File) {
    // let (mut tx, mut rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(1500);
    // let (mut tx, mut rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) =
    let (mut tx, mut rx) = crossbeam_channel::bounded(1000000);
    // let (mut tx, mut rx) = crossbeam_channel::unbounded();
    // let (mut tx, mut rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = channel();

    let t1 = std::thread::spawn(move || {
        let mut buffered_reader = std::io::BufReader::new(file);
        let now = std::time::Instant::now();

        loop {
            let mut buf = Vec::with_capacity(100);
            let t1 = std::time::Instant::now();
            let Ok(bytes_read) = buffered_reader.read_until(b'\n', &mut buf) else {
                break;
            };
            let seconds_for_reading = t1.elapsed().as_nanos();
            if bytes_read == 0 {
                break;
            }
            println!("read the line in {seconds_for_reading}");
            let t1 = std::time::Instant::now();
            // println!("read {} bytes", bytes_read);
            tx.send(buf).expect("could not send to the Receiver");
            let t2 = t1.elapsed().as_nanos();
            println!("sent the line to channel in {t2} seconds");
        }
        let time = now.elapsed().as_nanos();
        println!("read the file in {time} seconds");
    });

    let t2 = std::thread::spawn(move || {
        let mut map: HashMap<String, StationProps> = HashMap::new();
        while let Ok(buf) = rx.recv() {
            let t1 = std::time::Instant::now();
            let mut index: usize = 1;
            let bytes = buf.as_slice();
            let n = bytes.len();
            loop {
                if bytes[index] == b';' {
                    break;
                }
                index += 1
            }
            let station_name = String::from_utf8(bytes[0..index].to_vec()).unwrap();
            // println!("station_name: {}", station_name);
            let measurement = String::from_utf8(bytes[index + 1..n - 1].to_vec())
                .unwrap()
                .parse::<f32>()
                .unwrap();

            if map.contains_key(&station_name) {
                let station_props = map.get_mut(&station_name).unwrap();
                if measurement < station_props.min {
                    station_props.min = measurement
                }
                if measurement > station_props.max {
                    station_props.max = measurement
                }
                station_props.count += 1;
                station_props.sum += measurement;
            } else {
                let mut station_props = StationProps::default();
                station_props.min = measurement;
                station_props.max = measurement;
                station_props.count = 1;
                station_props.sum = measurement;
                map.insert(station_name, station_props);
            }
            let t2 = t1.elapsed().as_nanos();
            println!("processing took {t2} seconds");
        }
        let mut keys = map.keys().collect::<Vec<&String>>();
        keys.sort();
        for key in keys.iter() {
            let props = map.get(*key).unwrap();
            print!(
                "{}={}/{}/{},",
                *key,
                props.min,
                (props.sum / props.count as f32),
                props.max,
            )
        }
    });

    t1.join().expect("t1 could not be joined");
    t2.join().expect("t1 could not be joined");
}

fn single_producer_single_consumer(file: File) {
    let (mut tx, mut rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(1500);
    // let (mut tx, mut rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) =
    // let (mut tx, mut rx) = crossbeam_channel::bounded(100000);
    // let (mut tx, mut rx) = crossbeam_channel::unbounded();
    // let (mut tx, mut rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = channel();

    let t1 = std::thread::spawn(move || {
        let mut buffered_reader = std::io::BufReader::new(file);

        loop {
            let mut buf = Vec::with_capacity(100);
            let Ok(bytes_read) = buffered_reader.read_until(b'\n', &mut buf) else {
                break;
            };
            if bytes_read == 0 {
                break;
            }
            // println!("read {} bytes", bytes_read);
            tx.send(buf).expect("could not send to the Receiver");
        }
    });

    let t2 = std::thread::spawn(move || {
        let mut map: HashMap<String, StationProps> = HashMap::new();
        while let Ok(buf) = rx.recv() {
            let mut index: usize = 1;
            let bytes = buf.as_slice();
            let n = bytes.len();
            loop {
                if bytes[index] == b';' {
                    break;
                }
                index += 1
            }
            let station_name = String::from_utf8(bytes[0..index].to_vec()).unwrap();
            // println!("station_name: {}", station_name);
            let measurement = String::from_utf8(bytes[index + 1..n - 1].to_vec())
                .unwrap()
                .parse::<f32>()
                .unwrap();

            if map.contains_key(&station_name) {
                let station_props = map.get_mut(&station_name).unwrap();
                if measurement < station_props.min {
                    station_props.min = measurement
                }
                if measurement > station_props.max {
                    station_props.max = measurement
                }
                station_props.count += 1;
                station_props.sum += measurement;
            } else {
                let mut station_props = StationProps::default();
                station_props.min = measurement;
                station_props.max = measurement;
                station_props.count = 1;
                station_props.sum = measurement;
                map.insert(station_name, station_props);
            }
        }
        let mut keys = map.keys().collect::<Vec<&String>>();
        keys.sort();
        for key in keys.iter() {
            let props = map.get(*key).unwrap();
            print!(
                "{}={}/{}/{},",
                *key,
                props.min,
                (props.sum / props.count as f32),
                props.max,
            )
        }
    });

    t1.join().expect("t1 could not be joined");
    t2.join().expect("t1 could not be joined");
}

fn most_basic_implementation_v2_more_heap_allocation(file: File) {
    let mut map: HashMap<String, StationProps> = HashMap::new();
    let mut buffered_reader = std::io::BufReader::new(file);

    loop {
        let mut buf = Vec::with_capacity(100);
        // Reading into bytes as opposed to string.
        let t1 = std::time::Instant::now();
        let Ok(bytes_read) = buffered_reader.read_until(b'\n', &mut buf) else {
            break;
        };
        // let t2 = t1.elapsed().as_nanos();
        // println!(">>> read the line in {t2} nanos");

        if bytes_read == 0 {
            break;
        }
        let mut index: usize = 1;
        let bytes = buf.as_slice();
        let n = bytes.len();
        loop {
            if bytes[index] == b';' {
                break;
            }
            index += 1
        }
        let station_name = String::from_utf8(bytes[0..index].to_vec()).unwrap();
        let measurement = String::from_utf8(bytes[index + 1..n - 1].to_vec())
            .unwrap()
            .parse::<f32>()
            .unwrap();

        if map.contains_key(&station_name) {
            let station_props = map.get_mut(&station_name).unwrap();
            if measurement < station_props.min {
                station_props.min = measurement
            }
            if measurement > station_props.max {
                station_props.max = measurement
            }
            station_props.count += 1;
            station_props.sum += measurement;
        } else {
            let mut station_props = StationProps::default();
            station_props.min = measurement;
            station_props.max = measurement;
            station_props.count = 1;
            station_props.sum = measurement;
            map.insert(station_name, station_props);
        }
        buf.clear();
        let t2 = t1.elapsed().as_nanos();
        println!(">>> one whole round took {t2} seconds");
    }

    let mut keys = map.keys().collect::<Vec<&String>>();
    keys.sort();
    for key in keys.iter() {
        let props = map.get(*key).unwrap();
        print!(
            "{}={}/{}/{},",
            *key,
            props.min,
            (props.sum / props.count as f32),
            props.max,
        )
    }
}

fn some_single_thread_enhancements(mut file: File) {
    let mut map: HashMap<String, StationProps> = HashMap::new();
    println!("calculating size");
    let size = file.metadata().unwrap().len();
    println!("size: {size}");
    println!("will start reading");

    // let mut buf: Vec<u8> = vec![0; size as usize]; // TODO: for some reason, if i add the
    // capacity, the buf is filled with all zeroes.
    let mut buf: Vec<u8> = vec![];
    file.read_to_end(&mut buf).unwrap();
    let mut slice = buf.as_slice();
    println!(">>> reading ended");

    loop {
        if slice.len() < 1 {
            break;
        }
        let mut index: usize = 0;

        while slice[index] != b';' {
            index += 1;
        }

        let station_name = String::from_utf8_lossy(&slice[0..index]).to_string();

        slice = &slice[index + 1..];

        index = 0;
        while slice[index] != b'\n' {
            index += 1;
        }

        let measurement = String::from_utf8_lossy(&slice[0..index])
            .parse::<f32>()
            .unwrap();

        if map.contains_key(&station_name) {
            let station_props = map.get_mut(&station_name).unwrap();
            if measurement < station_props.min {
                station_props.min = measurement
            }
            if measurement > station_props.max {
                station_props.max = measurement
            }
            station_props.count += 1;
            station_props.sum += measurement;
        } else {
            let mut station_props = StationProps::default();
            station_props.min = measurement;
            station_props.max = measurement;
            station_props.count = 1;
            station_props.sum = measurement;
            map.insert(station_name, station_props);
        }
        slice = &slice[index + 1..];
        // buf.clear();
    }

    let mut keys = map.keys().collect::<Vec<&String>>();
    let keys_count = keys.len();
    println!("keys_count: {keys_count}");
    keys.sort();
    for key in keys.iter() {
        let props = map.get(*key).unwrap();
        print!(
            "{}={}/{}/{},",
            *key,
            props.min,
            (props.sum / props.count as f32),
            props.max,
        )
    }
}

fn most_basic_implementation_v2(mut file: File) {
    let mut map: HashMap<String, StationProps> = HashMap::new();
    // let size = file.stream_len().unwrap();
    println!("calculating size");
    let size = file.seek(std::io::SeekFrom::End(0)).unwrap();
    println!("size: {size}");
    println!("will start reading");
    let mut buf: Vec<u8> = vec![0; size as usize];
    file.read_to_end(&mut buf).unwrap();
    println!(">>> reading ended");

    let n = file.seek(std::io::SeekFrom::Start(0)).unwrap();
    println!("n: {n}");

    let mut buffered_reader = std::io::BufReader::new(file);
    let mut buf = Vec::with_capacity(100);

    loop {
        // Reading into bytes as opposed to string.
        let Ok(bytes_read) = buffered_reader.read_until(b'\n', &mut buf) else {
            break;
        };
        if bytes_read == 0 {
            break;
        }
        let mut index: usize = 1;
        let bytes = buf.as_slice();
        let n = bytes.len();
        loop {
            if bytes[index] == b';' {
                break;
            }
            index += 1
        }
        // TODO: remove to_vec
        // let station_name = String::from_utf8(bytes[0..index].to_vec()).unwrap();
        let station_name = String::from_utf8_lossy(&bytes[0..index]).to_string();
        let measurement = String::from_utf8_lossy(&bytes[index + 1..n - 1])
            .parse::<f32>()
            .unwrap();

        if map.contains_key(&station_name) {
            let station_props = map.get_mut(&station_name).unwrap();
            if measurement < station_props.min {
                station_props.min = measurement
            }
            if measurement > station_props.max {
                station_props.max = measurement
            }
            station_props.count += 1;
            station_props.sum += measurement;
        } else {
            let mut station_props = StationProps::default();
            station_props.min = measurement;
            station_props.max = measurement;
            station_props.count = 1;
            station_props.sum = measurement;
            map.insert(station_name, station_props);
        }
        buf.clear();
    }

    let mut keys = map.keys().collect::<Vec<&String>>();
    let keys_count = keys.len();
    println!("keys_count: {keys_count}");
    keys.sort();
    for key in keys.iter() {
        let props = map.get(*key).unwrap();
        print!(
            "{}={}/{}/{},",
            *key,
            props.min,
            (props.sum / props.count as f32),
            props.max,
        )
    }
}

fn most_basic_implementation(file: File) {
    let mut map: HashMap<String, StationProps> = HashMap::new();
    let mut buffered_reader = std::io::BufReader::new(file);
    let mut line = String::new();

    loop {
        let Ok(bytes_read) = buffered_reader.read_line(&mut line) else {
            break;
        };
        if bytes_read == 0 {
            break;
        }
        // println!("line: {line}"); // this includes the new line character too.
        let mut index: usize = 1;
        let bytes = line.as_bytes();
        let n = bytes.len();
        loop {
            if bytes[index] == b';' {
                break;
            }
            index += 1
        }
        let station_name = String::from_utf8(bytes[0..index].to_vec()).unwrap();
        let measurement = String::from_utf8(bytes[index + 1..n - 1].to_vec())
            .unwrap()
            .parse::<f32>()
            .unwrap();

        if map.contains_key(&station_name) {
            let station_props = map.get_mut(&station_name).unwrap();
            if measurement < station_props.min {
                station_props.min = measurement
            }
            if measurement > station_props.max {
                station_props.max = measurement
            }
            station_props.count += 1;
            station_props.sum += measurement;
        } else {
            let mut station_props = StationProps::default();
            station_props.min = measurement;
            station_props.max = measurement;
            station_props.count = 1;
            station_props.sum = measurement;
            map.insert(station_name, station_props);
        }
        line.clear()
    }

    let mut keys = map.keys().collect::<Vec<&String>>();
    keys.sort();
    for key in keys.iter() {
        let props = map.get(*key).unwrap();
        print!(
            "{}={}/{}/{},",
            *key,
            props.min,
            (props.sum / props.count as f32),
            props.max,
        )
    }
}
