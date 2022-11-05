import { createReadStream, PathLike } from "fs";
import split from "split2";
import { PassThrough, Readable } from "stream";
  // createBrotliDecompress,

type ParseNdJsonOption<T> = {
  parserFunction?: (data: T[], line: string) => T;
  reviver?: (key: string, value: any) => any;
  compressionAlgo?: "zip" | "gzip" | "brotli";
};

export const parseNdJsonStream = <T = any>(
  stream: Readable,
  options?: ParseNdJsonOption<T>
): Promise<T[]> => {
  return new Promise((resolve, reject) => {
    const result: T[] = [];

    stream
      .pipe(
        (() => {
          switch (options?.compressionAlgo) {
            case "zip":
              return createUnzip();
            case "gzip":
              return createGunzip();
            // case "brotli":
            //   return createBrotliDecompress();
            default:
              return new PassThrough();
          }
        })()
      )
      .pipe(
        split(line =>
          options?.parserFunction
            ? options.parserFunction(result, line)
            : JSON.parse(line, options?.reviver)
        )
      )
      .on("data", data => result.push(data))
      .on("finish", () => resolve(result))
      .on("error", reject);
  });
};

export const parseNdJsonFromFile = <T = any>(
  filename: PathLike,
  options?: ParseNdJsonOption<T>
): Promise<T[]> => {
  return parseNdJsonStream<T>(createReadStream(filename), options);
};
