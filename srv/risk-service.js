/**
 * Implementation for Risk Management service defined in ./risk-service.cds
 */
module.exports = async (srv) => {
    srv.after('READ', 'Risks', each => {
        if (each.impact >= 100000) {
        each.criticality = 1;
        } else {
        each.criticality = 2;
        }
        });

        const BupaService = await cds.connect.to('API_BUSINESS_PARTNER');
        srv.on('READ', srv.entities.BusinessPartners, async (req) => {
            return await BupaService.tx(req).run(req.query);
        });
}