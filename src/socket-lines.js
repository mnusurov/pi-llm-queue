// Newline-delimited JSON framing shared by the daemon and the lease client: streaming UTF-8
// decode, a 100 KB line cap (a peer that never sends a newline must not grow memory without
// bound), whitespace-only-line skip, and one JSON.parse per line. Callers own what a line means:
// `onInvalid` runs for a line that is not JSON, so the daemon can answer and the client can drop.

const MAX_LINE_LENGTH = 1e5;

/**
 * @param {import("node:net").Socket} socket
 * @param {(msg: any) => void} onMessage
 * @param {() => void} [onInvalid]
 */
export function readJsonLines(socket, onMessage, onInvalid = () => {}) {
	let buffer = "";
	// Streaming decoder: a chunk that splits a multi-byte character must not corrupt the line.
	const decoder = new TextDecoder("utf8");
	socket.on("data", (chunk) => {
		buffer += decoder.decode(chunk, { stream: true });
		if (buffer.length > MAX_LINE_LENGTH) {
			socket.destroy();
			return;
		}
		const lines = buffer.split("\n");
		buffer = lines.pop() ?? "";
		for (const line of lines) {
			if (!line.trim()) continue;
			let msg;
			try {
				msg = JSON.parse(line);
			} catch {
				onInvalid();
				continue;
			}
			onMessage(msg);
		}
	});
}
