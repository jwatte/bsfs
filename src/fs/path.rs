use std::path::PathBuf;

/// Expand an inode number to its storage path.
///
/// The path is constructed by taking progressively longer prefixes of the
/// decimal representation, each padded with zeros to the full length.
///
/// # Examples
///
/// ```
/// use bsfs::fs::expand_inode;
///
/// // inode 1234567 -> "1000000/1200000/1230000/1234000/1234500/1234560/1234567"
/// let path = expand_inode(1234567);
/// assert_eq!(path.to_str().unwrap(), "1000000/1200000/1230000/1234000/1234500/1234560/1234567");
///
/// // inode 3210 -> "3000/3200/3210/3210"
/// let path = expand_inode(3210);
/// assert_eq!(path.to_str().unwrap(), "3000/3200/3210/3210");
/// ```
pub fn expand_inode(n: u64) -> PathBuf {
    let s = n.to_string();
    let length = s.len();
    let mut parts = Vec::with_capacity(length);

    for i in 1..=length {
        let prefix = &s[..i];
        let padding = length - i;
        let padded = format!("{}{}", prefix, "0".repeat(padding));
        parts.push(padded);
    }

    PathBuf::from(parts.join("/"))
}

/// Get the full storage path for an inode within a data root directory
pub fn inode_data_path(data_root: &std::path::Path, inode: u64) -> PathBuf {
    data_root.join(expand_inode(inode))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_inode_example_1() {
        let path = expand_inode(1234567);
        assert_eq!(
            path.to_str().unwrap(),
            "1000000/1200000/1230000/1234000/1234500/1234560/1234567"
        );
    }

    #[test]
    fn test_expand_inode_example_2() {
        let path = expand_inode(3210);
        assert_eq!(path.to_str().unwrap(), "3000/3200/3210/3210");
    }

    #[test]
    fn test_expand_inode_single_digit() {
        let path = expand_inode(5);
        assert_eq!(path.to_str().unwrap(), "5");
    }

    #[test]
    fn test_expand_inode_two_digits() {
        let path = expand_inode(42);
        assert_eq!(path.to_str().unwrap(), "40/42");
    }

    #[test]
    fn test_expand_inode_root() {
        let path = expand_inode(1);
        assert_eq!(path.to_str().unwrap(), "1");
    }

    #[test]
    fn test_expand_inode_large() {
        let path = expand_inode(9876543210);
        let expected = "9000000000/9800000000/9870000000/9876000000/9876500000/9876540000/9876543000/9876543200/9876543210/9876543210";
        assert_eq!(path.to_str().unwrap(), expected);
    }

    #[test]
    fn test_inode_data_path() {
        let data_root = PathBuf::from("/data/bsfs");
        let path = inode_data_path(&data_root, 123);
        assert_eq!(path.to_str().unwrap(), "/data/bsfs/100/120/123");
    }
}

#[cfg(test)]
mod proptest_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_expand_inode_ends_with_number(n in 1u64..u64::MAX) {
            let path = expand_inode(n);
            let path_str = path.to_str().unwrap();
            // Path should end with the original number
            prop_assert!(path_str.ends_with(&n.to_string()));
        }

        #[test]
        fn test_expand_inode_parts_count(n in 1u64..u64::MAX) {
            let path = expand_inode(n);
            let path_str = path.to_str().unwrap();
            let parts: Vec<&str> = path_str.split('/').collect();
            // Number of parts should equal number of digits
            let expected_parts = n.to_string().len();
            prop_assert_eq!(parts.len(), expected_parts);
        }

        #[test]
        fn test_expand_inode_all_parts_same_length(n in 1u64..u64::MAX) {
            let path = expand_inode(n);
            let path_str = path.to_str().unwrap();
            let parts: Vec<&str> = path_str.split('/').collect();
            let expected_len = n.to_string().len();
            for part in parts {
                prop_assert_eq!(part.len(), expected_len);
            }
        }

        #[test]
        fn test_expand_inode_first_part_is_most_significant_digit_padded(n in 10u64..u64::MAX) {
            let path = expand_inode(n);
            let path_str = path.to_str().unwrap();
            let first_part = path_str.split('/').next().unwrap();
            let s = n.to_string();
            // First part should be first digit followed by zeros
            let expected_first = format!("{}{}", &s[..1], "0".repeat(s.len() - 1));
            prop_assert_eq!(first_part, expected_first);
        }
    }
}
