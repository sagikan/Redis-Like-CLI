use std::cmp::min;
use crate::db::{Database, ValueType};
use crate::client::{Client, Response};

pub async fn cmd_lrange(args: &[String], client: &Client, db: Database) {
    if args.len() != 3 {
        client.tx.send(Response::ErrArgCount.into()).unwrap();
        return;
    }

    // Extract key, start and stop args
    let key = &args[0];
    let (mut start, mut stop) = match (
        args[1].parse::<i32>(),
        args[2].parse::<i32>()
    ) {
        (Ok(start), Ok(stop)) => (start, stop),
        _ => {
            client.tx.send(Response::ErrNotInteger.into()).unwrap();
            return;
        }
    };

    match db.lock().await.get_mut(key) {
        Some(value) => match &mut value.val {
            ValueType::StringList(val_list) => { // An existing list is found
                // Adjust + clamp start and stop
                let val_list_len: i32 = val_list.len() as i32;
                let adjust = |x: i32| if x < 0 { val_list_len + x } else { x };
                start = adjust(start);
                stop = adjust(stop);
                if start < 0 { start = 0; }
                if stop < 0 { stop = -1; }
                // + Clamp stop to list range
                stop = min(stop, val_list_len - 1);

                if start > stop { // Invalid post-adjustment
                    client.tx.send(Response::EmptyArray.into()).unwrap();
                    return;
                }

                // Build bulk string
                let range_size = stop - start + 1;
                let mut bulk_str = format!("*{range_size}\r\n");
                for val in val_list.iter().skip(start as usize)
                                   .take(range_size as usize) {
                    bulk_str.push_str(&format!("${}\r\n{val}\r\n", val.len()));
                }

                client.tx.send(bulk_str.into_bytes()).unwrap();
            }, // Value is of the wrong type
            _ => client.tx.send(Response::WrongType.into()).unwrap()
        }, None => client.tx.send(Response::EmptyArray.into()).unwrap()
    }
}
