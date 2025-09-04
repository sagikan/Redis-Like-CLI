use std::cmp::min;
use crate::db::{Database, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_zrange(args: &[String], client: &Client, db: Database) {
    if args.len() != 3 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract set key and range
    let set_key = &args[0];
    let (mut start, mut stop) = match (
        args[1].parse::<i32>(),
        args[2].parse::<i32>()
    ) {
        (Ok(start), Ok(stop)) => (start, stop),
        _ => { // Unparsable
            client.tx.send(Response::ErrNotInteger.into()).unwrap();
            return;
        }
    };

    // Get + emit ranged set members
    let res = match db.lock().await.get(set_key) {
        Some(value) => match &value.val {
            ValueType::SortedSet(set) => { // An existing set is found
                // Adjust + clamp start and stop
                let set_len: i32 = set.len() as i32;
                let adjust = |x: i32| if x < 0 { set_len + x } else { x };
                start = adjust(start);
                stop = adjust(stop);
                if start < 0 { start = 0; }
                if stop < 0 { stop = -1; }
                // Clamp stop to set range
                stop = min(stop, set_len - 1);

                if start > stop { // Invalid range post-adjustment
                    client.tx.send(Response::EmptyArray.into()).unwrap();
                    return;
                }

                // Build array
                let range_size = stop - start + 1;
                let mut res = format!("*{range_size}\r\n");
                for set_member in set.iter().skip(start as usize).take(range_size as usize) {
                    res.push_str(&format!("${}\r\n{}\r\n", set_member.member.len(), set_member.member));
                }

                res.into_bytes()
            }, _ => Response::WrongType.into() // Value is of the wrong type
        }, None => Response::EmptyArray.into() // Set not found
    };

    client.tx.send(res).unwrap();
}
