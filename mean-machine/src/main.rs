#![allow(unused_variables, dead_code, unused_mut)]

use std::sync::mpsc::{sync_channel, Receiver, SyncSender};

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
    most_basic_implementation_v2(file);
}

#[derive(Debug, Default)]
struct StationProps {
    min: f32,
    max: f32,
    count: u32,
    sum: f32,
}

fn single_producer_single_consumer(file: File) {
    let (mut tx, mut rx): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(5);

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
    });

    t1.join().expect("t1 could not be joined");
    t2.join().expect("t1 could not be joined");
}

fn most_basic_implementation_v2(file: File) {
    let mut map: HashMap<String, StationProps> = HashMap::new();
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
