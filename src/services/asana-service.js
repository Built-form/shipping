const https = require('https');

/**
 * Fetches tasks from a specified Asana project.
 * @param {string} projectId - The ID of the Asana project.
 * @param {string} asanaPAT - The Asana Personal Access Token.
 * @returns {Promise<Array>} A promise that resolves to an array of Asana tasks.
 */
async function fetchAsanaProject(projectId, asanaPAT) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'app.asana.com',
            path: `/api/1.0/projects/${projectId}/tasks?opt_fields=name,custom_fields,created_at`,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${asanaPAT}`,
            },
        };

        https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    resolve(JSON.parse(data).data || []);
                } catch (err) {
                    reject(err);
                }
            });
        }).on('error', reject).end();
    });
}

module.exports = {
    fetchAsanaProject,
};