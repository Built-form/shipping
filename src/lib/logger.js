function formatLog(level, args) {
    const timestamp = new Date().toISOString();
    const msg = args.map(a => {
        if (typeof a === 'string') return a;
        if (a instanceof Error) return a.stack || a.message;
        return JSON.stringify(a);
    }).join(' ');
    return `[${timestamp}] [${level}]  ${msg}`;
}

module.exports = {
    info: (...args) => console.log(formatLog('INFO', args)),
    warn: (...args) => console.warn(formatLog('WARN', args)),
    error: (...args) => console.error(formatLog('ERROR', args)),
};
