/**
 * When one worker runs several invocations at once, its stdout is a single
 * stream. The worker runtime prefixes each console line with the request it
 * belongs to, and the parent splits the stream back out here.
 *
 * Shared by `support/nodejs-runtime` (writer) and `runtime/workers.ts` (reader).
 */

const MARK = "\u001e"; // ASCII record separator, never appears in normal logs

export function tagLine(requestID: string, line: string): string {
  return `${MARK}${requestID}${MARK}${line}`;
}

export interface AttributedChunk {
  requestID?: string;
  text: string;
}

/**
 * Split a stdout chunk into runs of consecutive lines that belong to the same
 * request. Lines without a tag get `requestID: undefined` so the caller can
 * fall back to whatever it knows about the worker.
 */
export function splitAttributed(chunk: string): AttributedChunk[] {
  const out: AttributedChunk[] = [];
  const lines = chunk.split("\n");
  // A trailing newline yields an empty last element; drop it so we do not
  // emit an empty untagged chunk.
  if (lines.length > 1 && lines[lines.length - 1] === "") lines.pop();

  for (const raw of lines) {
    let requestID: string | undefined;
    let text = raw;
    if (raw.startsWith(MARK)) {
      const end = raw.indexOf(MARK, 1);
      if (end > 1) {
        requestID = raw.slice(1, end);
        text = raw.slice(end + 1);
      }
    }
    const last = out[out.length - 1];
    if (last && last.requestID === requestID) {
      last.text += "\n" + text;
    } else {
      out.push({ requestID, text });
    }
  }
  return out;
}
