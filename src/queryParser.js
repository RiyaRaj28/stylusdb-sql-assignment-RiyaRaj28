function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        if (match) {
            const [, field, operator, value] = match;
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}


function parseQuery(query) {
    query = query.trim();
    let selectPart, joinPart; 

    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0];

    const whereClause = whereSplit.length>1 ? whereSplit[1].trim() : null;    //assigned empty array here

    const joinSplit = query.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
    selectPart = joinSplit[0].trim(); 
  
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;         //have
    const selectMatch = selectPart.match(selectRegex);
    if(!selectMatch){
        throw new Error('Invalid SELECT format');
    }
    const[, fields , table] = selectMatch;  


    const{ joinType, joinTable, joinCondition } = parseJoinClause(query); 

    let whereClauses = [];
    if(whereClause){
        whereClauses = parseWhereClause(whereClause); 
    }


    return{
        fields : fields.split(',').map(field => field.trim()),
        table : table.trim(),
        whereClauses,
        joinType,
        joinTable,
        joinCondition,
    }; 

}

module.exports = { parseQuery, parseJoinClause };



















