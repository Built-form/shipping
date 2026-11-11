'use strict';

// The info-bubble rules ShipLine must agree with, as data.
//
//   node tools/gen-ui-hints-contract.js           print the contract
//   node tools/gen-ui-hints-contract.js --write   write docs/frontend/ui-hints-contract.json
//
// The server's rules live in src/lib/ui-hints.js. ShipLine builds hint keys and
// checks bodies and links before it sends them (src/hints/domain.ts, links.ts)
// and keeps a copy of the written file at src/hints/ui-hints-contract.json.
// Both repos test against it: tools/test-ui-hints-lib.js fails when the file
// here is not what the rules build today, and ShipLine's contract.test.ts
// fails when its client rules disagree with its copy, or when its copy differs
// from this one (checked when the two clones sit side by side).
//
// Change a rule → run with --write → copy the file to ShipLine/src/hints/.

const fs = require('node:fs');
const path = require('node:path');
const L = require('../src/lib/ui-hints');

const CONTRACT_FILE = path.join(__dirname, '..', 'docs', 'frontend', 'ui-hints-contract.json');

const KEY_CASES = [
    'a/b',
    'payments.ready/col.supplier',
    'order/field.carton-cbm',
    'draft.plan/candidates/col.mo_ex_sea',
    'stages/stage.in_transit',
    'a.b.c.d.e/w/x/y/z',
    `a/${'x'.repeat(61)}`,
    `${'a'.repeat(60)}.${'b'.repeat(60)}/${'c'.repeat(38)}`,
    '',
    'payments',
    'Payments/col.x',
    'payments/',
    '/col.x',
    'payments//x',
    'a.b.c.d.e.f/x',
    'a/b/c/d/e/f',
    'a/-x',
    'a/.x',
    '-a/x',
    'a b/c',
    'a/é',
    `a/${'x'.repeat(62)}`,
    `${'a'.repeat(60)}.${'b'.repeat(60)}/${'c'.repeat(39)}`,
];

const BODY_CASES = [
    'Plain sentence.',
    '  one\r\ntwo\rthree  ',
    'x'.repeat(600),
    'x'.repeat(601),
    '',
    '   \n  ',
];

const LINK_CASES = [
    {},
    { linkPath: '', linkLabel: '  ' },
    { linkPath: '/payments?tab=ready', linkLabel: '  Ready to pay ' },
    { linkPath: '/info', linkLabel: 'All hints' },
    { linkPath: '/qc-visits?tab=visits&vst=CONFIRMED', linkLabel: 'Confirmed visits' },
    { linkPath: '/payments', linkLabel: 'x'.repeat(80) },
    { linkPath: '/payments' },
    { linkLabel: 'Payments' },
    { linkPath: 'payments', linkLabel: 'x' },
    { linkPath: '//evil.example', linkLabel: 'x' },
    { linkPath: 'https://evil.example/payments', linkLabel: 'x' },
    { linkPath: '/qc', linkLabel: 'x' },
    { linkPath: '/supplier-changes', linkLabel: 'x' },
    { linkPath: '/nope', linkLabel: 'x' },
    { linkPath: '/payments/../users', linkLabel: 'x' },
    { linkPath: '/%2e%2e/users', linkLabel: 'x' },
    { linkPath: '/payments%zz', linkLabel: 'x' },
    { linkPath: '/payments\\x', linkLabel: 'x' },
    { linkPath: '/pay ments', linkLabel: 'x' },
    { linkPath: `/payments?${'x'.repeat(200)}`, linkLabel: 'x' },
    { linkPath: '/payments', linkLabel: 'x'.repeat(81) },
];

const DELETE_CASES = [
    { userType: 'admin', userEmail: 'someone@jfa.test', createdBy: 'Maria@JFA.test' },
    { userType: 'standard', userEmail: 'maria@jfa.test', createdBy: 'Maria@JFA.test' },
    { userType: 'standard', userEmail: 'tom@jfa.test', createdBy: 'Maria@JFA.test' },
    { userType: 'accountant', userEmail: 'maria@jfa.test', createdBy: 'Maria@JFA.test' },
    { userType: 'warehouse', userEmail: 'maria@jfa.test', createdBy: 'Maria@JFA.test' },
    { userType: 'exec', userEmail: 'maria@jfa.test', createdBy: 'Maria@JFA.test' },
];

function buildContract() {
    return {
        contract: 1,
        source: 'shipping/src/lib/ui-hints.js via shipping/tools/gen-ui-hints-contract.js — do not edit by hand',
        keyRe: L.HINT_KEY_RE.source,
        limits: {
            key: L.HINT_KEY_MAX_LEN,
            body: L.HINT_BODY_MAX_LEN,
            linkPath: L.LINK_PATH_MAX_LEN,
            linkLabel: L.LINK_LABEL_MAX_LEN,
        },
        roles: [...L.HINT_ROLES],
        linkRoots: [...L.LINK_ROOTS],
        refusals: Object.fromEntries(
            [...Object.values(L.REFUSALS), L.cannotWrite('accountant')].map(r => [r.code, { status: r.status }]),
        ),
        cases: {
            keys: KEY_CASES.map(key => ({ key, ok: !L.parseKey(key).error })),
            bodies: BODY_CASES.map(body => {
                const r = L.parseBody(body);
                return r.error ? { body, ok: false } : { body, ok: true, normalized: r.body };
            }),
            links: LINK_CASES.map(input => {
                const r = L.parseLink(input);
                return r.error ? { input, ok: false } : { input, ok: true, linkPath: r.linkPath, linkLabel: r.linkLabel };
            }),
            deletes: DELETE_CASES.map(c => ({ ...c, ok: L.canDeleteHint(c.userType, c.userEmail, { created_by: c.createdBy }) })),
        },
    };
}

const render = () => `${JSON.stringify(buildContract(), null, 2)}\n`;

if (require.main === module) {
    if (process.argv.includes('--write')) {
        fs.writeFileSync(CONTRACT_FILE, render());
        console.log(`wrote ${path.relative(process.cwd(), CONTRACT_FILE)}`);
    } else {
        process.stdout.write(render());
    }
}

module.exports = { buildContract, render, CONTRACT_FILE };
