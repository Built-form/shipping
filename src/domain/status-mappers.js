function mapGoodsOnSeaStatus() {
    return 'ON_SEA';
}

function mapGoodsOnAirStatus() {
    return 'ON_AIR';
}

function mapOrdersStatus(goodsStatus, artworkSentDate) {
    const s = (goodsStatus || '').toLowerCase().trim();
    if (s === 'partial ready') return 'PARTIAL_READY';
    if (s === 'ready') return 'READY';
    if (artworkSentDate) return 'IN_PRODUCTION';
    return 'PO_SENT';
}

module.exports = { mapGoodsOnSeaStatus, mapGoodsOnAirStatus, mapOrdersStatus };
