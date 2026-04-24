/**
 * Transforms Asana tasks into a CSV-like format, using custom field names as keys.
 * @param {Array} tasks - An array of Asana task objects.
 * @returns {Array} An array of objects, where each object represents a row with transformed data.
 */
function transformAsanaToCSV(tasks) {
    return tasks.map(task => {
        const row = { 'Name': task.name };

        if (task.custom_fields) {
            task.custom_fields.forEach(field => {
                if (field.display_value != null) {
                    row[field.name] = field.display_value.trim();
                } else if (field.number_value != null) {
                    row[field.name] = String(field.number_value);
                } else {
                    row[field.name] = '';
                }
            });
        }
        return row;
    });
}

module.exports = {
    transformAsanaToCSV,
};