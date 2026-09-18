import { describe, expect, it } from "vitest";
import { splitAttributed, tagLine } from "../../src/runtime/stdout-attribution.js";

describe("stdout attribution", () => {
  it("passes untagged output through", () => {
    expect(splitAttributed("hello\nworld\n")).toEqual([
      { requestID: undefined, text: "hello\nworld" },
    ]);
  });

  it("attributes tagged lines to their request", () => {
    const chunk = [tagLine("req-a", "one"), tagLine("req-a", "two"), tagLine("req-b", "three")].join("\n") + "\n";
    expect(splitAttributed(chunk)).toEqual([
      { requestID: "req-a", text: "one\ntwo" },
      { requestID: "req-b", text: "three" },
    ]);
  });

  it("keeps untagged lines between tagged ones separate", () => {
    const chunk = [tagLine("req-a", "one"), "raw", tagLine("req-a", "two")].join("\n");
    expect(splitAttributed(chunk)).toEqual([
      { requestID: "req-a", text: "one" },
      { requestID: undefined, text: "raw" },
      { requestID: "req-a", text: "two" },
    ]);
  });

  it("does not choke on a bare marker", () => {
    expect(splitAttributed("\u001e")).toEqual([{ requestID: undefined, text: "\u001e" }]);
  });
});
