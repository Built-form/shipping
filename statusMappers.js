/**
 * Maps the status for "Goods on Sea" items.
 * @returns {string} The mapped status.
 */
function mapGoodsOnSeaStatus() {
    return 'ON_SEA';
}

/**
 * Maps the status for general orders based on the goods status.
 * @param {string} goodsStatus - The raw goods status from Asana.
 * @returns {string} The mapped order status.
 */
function mapOrdersStatus(goodsStatus) {
    const s = (goodsStatus || '').toLowerCase().trim();
    if (s.includes('ready')) return 'READY_AT_FACTORY';
    return 'PO_SENT';
}

module.exports = {
    mapGoodsOnSeaStatus,
    mapOrdersStatus,
};