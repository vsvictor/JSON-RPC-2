use super::*;

#[test]
fn legacy_jsrt_roundtrip_encrypts_and_decrypts() {
    let crypto = SrtCrypto::new(&SrtConfig {
        passphrase: "secret-passphrase".to_owned(),
    })
    .unwrap();
    let packet = SrtPacket::request(7, 11, b"hello".to_vec());
    let encoded = crypto.encode_packet(&packet).unwrap();
    assert_eq!(crypto.decode_packet(&encoded).unwrap(), packet);
    assert_eq!(&encoded[..4], b"JSRT");
}

#[test]
fn native_srt_passphrase_validation_matches_libsrt_byte_limits() {
    for length in [0, 1, 9, 80, 81] {
        assert!(
            SrtConfig {
                passphrase: "x".repeat(length)
            }
            .validate()
            .is_err()
        );
    }
    for length in [10, 11, 78, 79] {
        assert!(
            SrtConfig {
                passphrase: "x".repeat(length)
            }
            .validate()
            .is_ok()
        );
    }
    assert!(SrtConfig::default().validate().is_err());
    assert!(
        SrtConfig {
            passphrase: "\u{00e9}".repeat(5)
        }
        .validate()
        .is_ok()
    );
    assert!(
        SrtConfig {
            passphrase: "\u{00e9}".repeat(40)
        }
        .validate()
        .is_err()
    );
}
