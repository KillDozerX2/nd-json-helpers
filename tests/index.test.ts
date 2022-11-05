import { assert, expect } from "chai";
import "mocha";
import * as path from "path";
import { parseNdJsonFromFile } from "../src/index";

describe("File Parse NdJson Files", () => {
  it("Will successfully parse File stream", async () => {
    const data = await parseNdJsonFromFile(path.join(__dirname, "data.ndjson"));
    assert.isArray(data);
    expect(data.length).to.equal(100);
  });
  it("Will successfully parse GZip file", async () => {
    const data = await parseNdJsonFromFile(
      path.join(__dirname, "data.ndjson.gz"),
      {
        compressionAlgo: "gzip",
      }
    );
    assert.isArray(data);
    expect(data.length).to.equal(100);
  });
});
