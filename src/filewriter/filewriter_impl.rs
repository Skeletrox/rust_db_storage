use rust_db_proto::proto::Datatype;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

pub struct FileWriter {
    path: String,
    // UNUSED for now, will be useful for generated code
    _datatype: Datatype
}

impl FileWriter {
    pub fn new(path: &str, datatype: Datatype) -> FileWriter {
        FileWriter {
            path: path.to_string(),
            _datatype: datatype
        }
    }

    pub async fn write_data(&self, data: &[u8]) -> std::io::Result<()> {
        let mut file = File::create(self.path.as_str()).await?;
        file.write_all(data).await?;
        file.flush().await?;

        Ok(())
    }

    pub fn generate_str_buf(&self, data_vec: Vec<String>) -> Vec<u8> {
        // For each string: convert to bytes and add the size as padding in
        // the size field. Each field is therefore defined as [bytes][value]
        // with no delimiting (kinda scary but ok)
        // TODO(@Skeletrox): Use macros to make this generated code
        let mut bytes: Vec<u8> = Vec::new();

        for entry in data_vec {
            // Encode to UTF-8
            let mut curr_bytes = entry.as_bytes().to_vec();
            let mut len_bytes = curr_bytes.len().to_ne_bytes().to_vec();
            bytes.append(&mut len_bytes);
            bytes.append(&mut curr_bytes);
        }

        return bytes;
    }

    pub async fn write_str_vec(&self, data_vec: Vec<String>) -> std::io::Result<()> {
        let data = self.generate_str_buf(data_vec);
        self.write_data(&data).await?;

        Ok(())
    }
}