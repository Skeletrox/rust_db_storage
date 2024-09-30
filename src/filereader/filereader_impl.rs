use rust_db_proto::proto::Datatype;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub struct FileReader {
    path: String,
    // UNUSED for now, will be useful for generated code
    _datatype: Datatype
}

impl FileReader {
    pub fn new(path: &str, datatype: Datatype) -> FileReader {
        FileReader {
            path: path.to_string(),
            _datatype: datatype
        }
    }

    pub async fn read_data(&self) -> std::io::Result<Vec<u8>> {
        let mut file = File::open(
            self.path.as_str()).await?;
        let mut content: Vec<u8> = Vec::new();
        file.read_to_end(&mut content).await?;

        Ok(content)
    }

    pub async fn parse_to_str_vec(&self) -> std::io::Result<Vec<String>> {
        let data = self.read_data().await?;
        let mut returnable: Vec<String> = Vec::new();
        // We know that the data is stored as follows:
        // [size][data]
        let usize_size = std::mem::size_of::<usize>();
        let data_as_slice = data.as_slice();
        let mut index = 0;
        // boolean for checking if we are reading the size of data, or the
        // data itself
        let mut is_reading_size = true;
        // Current size of data being read, if any
        let mut curr_size = 0;
        while index < data_as_slice.len() {
            let rhs;
            if is_reading_size {
                rhs = index + usize_size;
                // Since we cannot guarantee that the array is going
                // to have 8 bytes, we need to run try_into()
                curr_size = usize::from_ne_bytes(
                    data_as_slice[index..rhs].try_into().unwrap());
                    is_reading_size = false;
            } else {
                rhs = index + curr_size;
                let curr_value = String::from_utf8_lossy(
                    &data_as_slice[index..rhs]);
                returnable.push(curr_value.to_string());
                is_reading_size = true;
            }
            index += rhs;
        }
        Ok(returnable)
    }
}