use actix::fut::future::WrapFuture;
use actix::{Actor, Handler, Message, ResponseActFuture, StreamHandler};
use actix::{ActorFutureExt, Context};
use actix_web::{cookie::time::Time, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use futures::stream::TryStreamExt;
use mongodb::{
    bson,
    options::{ClientOptions, FindOptions},
    Client,
};
use serde::{Deserialize, Serialize};
use std::future::IntoFuture;

use actix::prelude::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Item {
    timestamp: bson::DateTime,
    I130_lon_deg: f64,
    I030_tod_s: f64,
    I140_galt_ft: f64,
    I145_fl: f64,
    I155_bvr_ftpm: f64,
    I160_gs_nmps: f64,
    I130_lat_deg: f64,
    I160_ta_deg: f64,
}

struct Database(Client);

impl Clone for Database {
    fn clone(&self) -> Self {
        Database(self.0.clone())
    }
}

impl Database {
    fn ping(&self) {
        let client = self.0.clone();

        tokio::spawn(async move {
            client
                .database("admin")
                .run_command(bson::doc! {"ping": 1}, None)
                .await
                .expect("failed to ping Database");
            println!("ping success!");
        });
    }
}

impl Actor for Database {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut <Self as Actor>::Context) {
        self.ping();
        ctx.text("welcome")
    }
}

// TODO: sample code from https://actix.rs/docs/websockets

/// Handler for ws::Message message
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Database {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                dbg!(&msg);
                ctx.pong(&msg)
            }
            Ok(ws::Message::Text(text)) => {
                dbg!(&text);
                ctx.text(format!("echoing the following: {}", text))
            }
            Ok(ws::Message::Binary(bin)) => {
                dbg!(&bin);
            }
            Ok(whatever) => {
                dbg!(&whatever);
            }
            Err(e) => {
                dbg!(e.to_string());
            }
        }
    }
}

async fn index(
    client: web::Data<Database>,
    req: HttpRequest,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    let client = client.get_ref().clone();
    let resp = ws::start(client, &req, stream);
    println!("{:?}", resp);
    resp
}

#[derive(Clone)]
struct DatabaseSimple {
    client: Client,
}

impl Actor for DatabaseSimple {
    type Context = Context<Self>;
}

#[derive(Message)]
#[rtype(result = "Result<Vec<Item>, Error>")]
pub struct TimeRange {
    start_epoch: String,
    end_epoch: String,
}

impl Handler<TimeRange> for DatabaseSimple {
    type Result = Vec<Item>;

    fn handle(&mut self, msg: TimeRange, _ctx: &mut Self::Context) -> Self::Result {
        let client = self.client.clone();

        Box::pin(async move {
            let collection = client.database("romeo5").collection::<Item>("asterix");

            let date_init = bson::DateTime::parse_rfc3339_str(msg.start_epoch).unwrap();

            let date_end = bson::DateTime::parse_rfc3339_str(msg.end_epoch).unwrap();

            let filter = bson::doc! {
                "timestamp": {
                    "$gte" : date_init ,
                    "$lt" : date_end,
                }
            };

            let filter_ref = filter.clone();

            let find_options = FindOptions::builder().build();

            let mut cursor = collection.find(filter_ref, find_options).await.unwrap();

            let mut res = Vec::<Item>::new();
            // Iterate over the results of the cursor.
            while let Some(item) = cursor.try_next().await.unwrap() {
                res.push(item);
            }
        });
        let mut res = Vec::<Item>::new();
        res
    }
}

async fn index_get(
    address: web::Data<Addr<DatabaseSimple>>,
    req: HttpRequest,
) -> Result<HttpResponse, Error> {
    let range = TimeRange {
        start_epoch: String::from("2022-11-03T12:57:18.123Z"),
        end_epoch: String::from("2022-11-03T12:57:20.123Z"),
    };

    address.send(range);

    Ok(HttpResponse::Ok().finish())
}

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let client_options = ClientOptions::parse(
        "mongodb://localhost:27017/romeo5?readPreference=primary&ssl=false&directConnection=true",
    )
    .await?;

    let client = Client::with_options(client_options)?;

    client
        .database("admin")
        .run_command(bson::doc! {"ping": 1}, None)
        .await?;
    println!("Connected and ping success!");

    for db_name in client.list_database_names(None, None).await? {
        println!("{}", db_name);
    }

    let collection = client.database("romeo5").collection::<Item>("asterix");

    let date_init = bson::DateTime::parse_rfc3339_str("2022-11-03T12:57:18.123Z").unwrap();

    let date_end = bson::DateTime::parse_rfc3339_str("2022-11-03T12:57:20.123Z").unwrap();

    let filter = bson::doc! {
        "timestamp": {
            "$gte" : date_init ,
            "$lt" : date_end,
        }
    };

    let filter_ref = filter.clone();

    let find_options = FindOptions::builder().build();

    let mut cursor = collection.find(filter_ref, find_options).await?;

    // Iterate over the results of the cursor.
    while let Some(item) = cursor.try_next().await? {
        println!("{:?}", item);
    }

    let db = Database(client.clone());

    let database_simple = DatabaseSimple {
        client: client.clone(),
    };

    let database_simple_address = database_simple.start();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(db.clone()))
            .app_data(web::Data::new(database_simple_address.clone()))
            .route("/ws", web::get().to(index))
            .route("/test", web::get().to(index_get))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await?;

    Ok(())
}
