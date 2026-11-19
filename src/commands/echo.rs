pub fn echo(message: &str) -> Vec<u8> {
    let mut response = Vec::new();
    response.extend_from_slice(b"$");
    response.extend_from_slice(&(message.len() as u64).to_string().into_bytes());
    response.extend_from_slice(b"\r\n");
    response.extend_from_slice(message.as_bytes());
    response.extend_from_slice(b"\r\n");
    response
}