/*
  The Event explorer's exports of a table, made in the browser: CSV, and a PDF written by hand (one built-in font,
  text only) so no library is needed. Both products offer both. Pure apart from download(), which needs a document.
*/

export type Table = { headers: string[]; rows: string[][] };

export function download(name: string, type: string, data: BlobPart) {
  const url = URL.createObjectURL(new Blob([data], { type }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/* RFC 4180: quote a cell that holds a comma, a quote or a line break. A cell that
   starts with = + - or @ gets a leading apostrophe, so a spreadsheet does not run it as a formula. */
export function toCsv({ headers, rows }: Table): string {
  const cell = (v: string) => {
    const s = /^[=+\-@]/.test(v) && !/^-?\d/.test(v) ? `'${v}` : v;
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return "﻿" + [headers, ...rows].map((r) => r.map(cell).join(",")).join("\r\n") + "\r\n";
}

/* A4 landscape, Courier. Columns are as wide as their longest value, and the widest are
   trimmed until the row fits the page. The heading lines and the column names repeat on every page. */
export function toPdf({ headers, rows }: Table, title: string, lines: string[]): ArrayBuffer {
  const W = 842, H = 595, M = 28, SIZE = 6.5, CH = SIZE * 0.6, LEAD = 9.5;
  const latin = (s: string) => s.replace(/[^\x20-\x7e\xa0-\xff]/g, "?");
  const esc = (s: string) => latin(s).replace(/[\\()]/g, "\\$&");
  const room = Math.floor((W - 2 * M) / CH);
  const widths = headers.map((h, i) => Math.max(h.length, ...rows.map((r) => (r[i] ?? "").length)));
  const total = () => widths.reduce((a, w) => a + w, 0) + 2 * (widths.length - 1);
  while (total() > room) { const i = widths.indexOf(Math.max(...widths)); if (widths[i] <= 6) break; widths[i]--; }
  const fit = (v: string, w: number) => (v.length > w ? `${v.slice(0, Math.max(1, w - 1))}~` : v.padEnd(w));
  const line = (r: string[]) => r.map((v, i) => fit(v ?? "", widths[i])).join("  ");

  const headRoom = M + 14 + lines.length * 10 + 8;
  const perPage = Math.max(1, Math.floor((H - headRoom - M - 22) / LEAD));
  const pages = Math.max(1, Math.ceil(rows.length / perPage));
  const streams: string[] = [];
  for (let p = 0; p < pages; p++) {
    let y = H - M - 10;
    const out: string[] = ["BT", `/F2 11 Tf 1 0 0 1 ${M} ${y} Tm (${esc(title)}) Tj`];
    for (const l of lines) { y -= 10; out.push(`/F1 7 Tf 1 0 0 1 ${M} ${y} Tm (${esc(l)}) Tj`); }
    y -= 16;
    out.push(`/F2 ${SIZE} Tf 1 0 0 1 ${M} ${y} Tm (${esc(line(headers))}) Tj`, `/F1 ${SIZE} Tf`);
    for (const r of rows.slice(p * perPage, (p + 1) * perPage)) { y -= LEAD; out.push(`1 0 0 1 ${M} ${y} Tm (${esc(line(r))}) Tj`); }
    out.push(`/F1 7 Tf 1 0 0 1 ${M} ${M - 8} Tm (Page ${p + 1} of ${pages}) Tj`, "ET");
    streams.push(out.join("\n"));
  }

  /* objects: 1 catalog, 2 pages, 3 and 4 fonts, then a page and its content for each page */
  const objs: string[] = [
    "<< /Type /Catalog /Pages 2 0 R >>",
    `<< /Type /Pages /Count ${pages} /Kids [${streams.map((_, i) => `${5 + i * 2} 0 R`).join(" ")}] >>`,
    "<< /Type /Font /Subtype /Type1 /BaseFont /Courier /Encoding /WinAnsiEncoding >>",
    "<< /Type /Font /Subtype /Type1 /BaseFont /Courier-Bold /Encoding /WinAnsiEncoding >>",
  ];
  streams.forEach((s, i) => {
    objs.push(`<< /Type /Page /Parent 2 0 R /MediaBox [0 0 ${W} ${H}] /Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> /Contents ${6 + i * 2} 0 R >>`);
    objs.push(`<< /Length ${s.length} >>\nstream\n${s}\nendstream`);
  });
  let pdf = "%PDF-1.4\n";
  const at: number[] = [];
  objs.forEach((o, i) => { at.push(pdf.length); pdf += `${i + 1} 0 obj\n${o}\nendobj\n`; });
  const xref = pdf.length;
  pdf += `xref\n0 ${objs.length + 1}\n0000000000 65535 f \n${at.map((n) => `${String(n).padStart(10, "0")} 00000 n \n`).join("")}`;
  pdf += `trailer\n<< /Size ${objs.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
  /* every character is one byte: the text was reduced to Latin-1 above */
  const bytes = new Uint8Array(new ArrayBuffer(pdf.length));
  for (let i = 0; i < pdf.length; i++) bytes[i] = pdf.charCodeAt(i) & 0xff;
  return bytes.buffer;
}
