'use strict';

// File types and Content-Disposition for QC files served through presigned
// GET URLs: visit attachments (src/services/qc-visit-routes.js) and the JFPRO
// kit documents (src/services/qc-kit-routes.js).
//
// No DB, no AWS.

// Types a presigned view URL may show inline. Anything else (HTML, SVG,
// unknown) is served as an application/octet-stream attachment, so a stored
// file can never render as a page on the bucket's origin.
const INLINE_TYPES = new Set(['image/jpeg', 'image/png', 'image/gif', 'image/webp', 'application/pdf', 'video/mp4']);
// Types accepted by the visit attachment upload-url route. SVG and HTML stay out.
const UPLOAD_TYPES = new Set([
    'image/jpeg', 'image/png', 'image/gif', 'image/webp', 'image/heic',
    'application/pdf', 'video/mp4', 'video/quicktime',
    'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/vnd.ms-powerpoint', 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'text/csv', 'text/plain', 'application/zip', 'application/x-zip-compressed', // Windows browsers report zips as the latter
]);
const baseType = t => String(t || '').split(';')[0].trim().toLowerCase();
const isUploadType = t => UPLOAD_TYPES.has(baseType(t));

// `inline|attachment; filename="<ascii>"; filename*=UTF-8''<utf-8>` with
// control characters, quotes and backslashes removed.
function contentDisposition(disposition, name) {
    // eslint-disable-next-line no-control-regex
    const clean = String(name == null ? '' : name).replace(/[\x00-\x1f\x7f"\\]/g, '') || 'file';
    const ascii = clean.replace(/[^\x20-\x7e]/g, '_');
    let utf8;
    try {
        utf8 = encodeURIComponent(clean).replace(/['()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
    } catch { utf8 = encodeURIComponent(ascii); } // lone surrogate
    return `${disposition}; filename="${ascii}"; filename*=UTF-8''${utf8}`;
}

// GetObject response overrides for a view (`url`) link: inline with the
// stored type for INLINE_TYPES, otherwise a download.
function viewResponseParams(filename, contentType) {
    const type = baseType(contentType);
    return INLINE_TYPES.has(type)
        ? { ResponseContentType: type, ResponseContentDisposition: contentDisposition('inline', filename) }
        : { ResponseContentType: 'application/octet-stream', ResponseContentDisposition: contentDisposition('attachment', filename) };
}

module.exports = { INLINE_TYPES, UPLOAD_TYPES, baseType, isUploadType, contentDisposition, viewResponseParams };
