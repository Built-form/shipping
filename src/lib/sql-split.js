'use strict';

// Split a .sql file into statements for tools/migrate.js.
//
// tools/sql.js splits on /;\s*/, so a semicolon inside a comment or a string
// cuts a statement in two (five files in src/db/migrations/ break that way).
// This splitter is a small state machine that only splits on a semicolon
// outside quotes and comments, and drops ordinary comments from the output so
// a comment-only chunk never reaches MySQL as an empty query. MySQL
// executable comments (/*! ... */) are kept. DELIMITER is a mysql-client
// command, not SQL, so a file using it is rejected: write trigger and
// procedure bodies as a single statement instead.

function splitSql(text) {
    const src = String(text).replace(/\r\n?/g, '\n');
    const statements = [];
    let cur = '';
    let i = 0;

    const flush = () => {
        const stmt = cur.trim();
        if (stmt) {
            if (/^DELIMITER\b/i.test(stmt)) {
                throw new Error('DELIMITER is not supported: write the body as a single statement');
            }
            statements.push(stmt);
        }
        cur = '';
    };

    while (i < src.length) {
        const c = src[i];
        const next = src[i + 1];

        // -- comment: MySQL needs whitespace (or end of input) after the dashes.
        if (c === '-' && next === '-' && (i + 2 >= src.length || /\s/.test(src[i + 2]))) {
            while (i < src.length && src[i] !== '\n') i++;
            continue;
        }
        if (c === '#') {
            while (i < src.length && src[i] !== '\n') i++;
            continue;
        }
        if (c === '/' && next === '*') {
            const end = src.indexOf('*/', i + 2);
            if (end < 0) throw new Error('unterminated /* comment');
            if (src[i + 2] === '!') cur += src.slice(i, end + 2);
            else cur += ' ';
            i = end + 2;
            continue;
        }
        if (c === '\'' || c === '"' || c === '`') {
            let j = i + 1;
            while (j < src.length) {
                if (src[j] === '\\' && c !== '`') { j += 2; continue; }
                if (src[j] === c) {
                    if (src[j + 1] === c) { j += 2; continue; } // doubled quote
                    break;
                }
                j++;
            }
            if (j >= src.length) throw new Error(`unterminated ${c} quote`);
            cur += src.slice(i, j + 1);
            i = j + 1;
            continue;
        }
        if (c === ';') {
            flush();
            i++;
            continue;
        }
        cur += c;
        i++;
    }
    flush();
    return statements;
}

module.exports = { splitSql };
