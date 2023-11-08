use std::io::{Read, Write};
use std::str;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use rand::Rng;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;
use redis::AsyncCommands;
 
async fn generate_short_link() -> String {
    let alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    let short_link: String = (0..9)
        .map(|_| {
            let idx = rng.gen_range(0..alphabet.len());
            alphabet.chars().nth(idx).unwrap()
        })
        .collect();
    short_link
}
 
 
async fn base_find_link(short_link: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await?;
    let msg = format!("HGET linksHashtable {}\r\n", short_link);
    stream.write_all(msg.as_bytes()).await?;
 
    let mut buffer = [0; 1024]; 
    let size = stream.read(&mut buffer).await?;
    let reply = String::from_utf8(buffer[..size].to_vec())?;
 
    if reply.contains("-1") || reply.contains("nil") {
        Err("Link does not exist".into())
    } else {
 
        let url = reply.split_whitespace().last().ok_or("No URL in reply")?.to_string();
        Ok(url)
    }
}
 
async fn base_add_link(short_link: &str, long_link: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut con = TcpStream::connect("127.0.0.1:6379").await?;
    let msg = format!("HSET linksHashtable {} {}\r\n", short_link, long_link);
    con.write_all(msg.as_bytes()).await?;
    Ok(())
}
 
async fn handle_request(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    // Выводим метод и путь запроса для всех запросов
    println!("Received a {} request to {}", req.method(), req.uri().path());
 
    if req.method() == hyper::Method::POST {
        let body_bytes = hyper::body::to_bytes(req.into_body()).await?;
        let long_url = match str::from_utf8(&body_bytes) {
            Ok(url) => url,
            Err(err) => {
                let response = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(format!("Invalid UTF-8: {}", err)))
                    .unwrap();
                return Ok(response);
            }
        };
 
        if long_url.is_empty() {
            let response = Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::empty())
                .unwrap();
            return Ok(response);
        }
 
        let short_url = generate_short_link().await;
        base_add_link(&short_url, long_url).await.unwrap();
 
        let response_body = format!("Short URL: 127.0.0.1:8080/{}", short_url);
        return Ok(Response::new(Body::from(response_body)));
    } else if req.method() == hyper::Method::GET {
        // Выводим информацию о GET-запросе
        println!("Handling GET request for path: {}", req.uri().path());
 
        let short_url = req.uri().path().trim_start_matches('/').to_string();
        println!("Extracted short URL: {}", short_url);
 
        match base_find_link(&short_url).await {
            Ok(result) => {
                println!("Link found for {}", short_url);
                if result.starts_with("http://") || result.starts_with("https://") {
                    let response = Response::builder()
                        .status(StatusCode::MOVED_PERMANENTLY)
                        .header(hyper::header::LOCATION, result.clone())
                        .body(Body::empty())
                        .unwrap();
                    return Ok(response);
                } else {
                    let response = Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(Body::empty())
                        .unwrap();
                    return Ok(response);
                }
            }
            Err(e) => {
                println!("Error retrieving link for {}: {}", short_url, e);
                let response = Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap();
                return Ok(response);
            }
        }
    } else {
        let response = Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .unwrap();
        return Ok(response);
    }
}
 
async fn initialize_base() -> Result<(), Box<dyn Error>> {
    let mut con = TcpStream::connect("127.0.0.1:6379").await?;
 
    let msg = "HSET linksHashtable _test initializationkey\r\n";
    con.write_all(msg.as_bytes()).await?;
 
    Ok(())
}
 
#[tokio::main]
async fn main() {
    let addr = ([127, 0, 0, 1], 8080).into();
    if let Err(e) = initialize_base().await {
        eprintln!("Initialization error: {}", e);
        return;
    }
 
    let make_svc = make_service_fn(|_conn| {
        async {
            Ok::<_, hyper::Error>(service_fn(handle_request))
        }
    });
    let server = Server::bind(&addr).serve(make_svc);
 
    if let Err(e) = server.await {
        eprintln!("Server error: {}", e);
    }
}