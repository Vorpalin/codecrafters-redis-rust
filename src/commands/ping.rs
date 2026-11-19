pub fn ping() -> &'static [u8; 7] {
    b"+PONG\r\n"
}
