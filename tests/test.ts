import * as fs from "fs"
import * as path from "path"
import { createBrotliCompress, createGzip } from "zlib"

(async () => {
  fs.createReadStream(path.join(__dirname, "data.ndjson"))
  .pipe(createGzip()).pipe(fs.createWriteStream(path.join(__dirname, "data.ndjson.gz")))
  fs.createReadStream(path.join(__dirname, "data.ndjson"))
    .pipe(createBrotliCompress()).pipe(fs.createWriteStream(path.join(__dirname, "data.ndjson.br")))
})()
