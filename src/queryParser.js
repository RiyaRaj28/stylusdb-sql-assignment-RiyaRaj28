function parseQuery(query) {
    query = query.trim();
    let selectPart, joinPart; 

    const whereSplit = query.split(/\sWHERE\s/i);
    //SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id 
    query = whereSplit[0];

    const whereClause = whereSplit.length>1 ? whereSplit[1].trim() : null;    //assigned empty array here
    //student.name = John

    const joinSplit = query.split(/\sINNER JOIN\s/i);
    selectPart = joinSplit[0].trim(); 
    //selectPart = SELECT student.name, enrollment.course FROM student

    joinPart = joinSplit.length>1 ? joinSplit[1].trim() : null;
    //joinPart = enrollment ON student.id=enrollment.student_id  

    //parse selectPart = SELECT student.name, enrollment.course FROM student
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;         //have
    const selectMatch = selectPart.match(selectRegex);
    if(!selectMatch){
        throw new Error('Invalid SELECT format');
    }
    const[, fields , table] = selectMatch;  

    //parse the join part if it exists
    //enrollment ON student.id=enrollment.student_id
    let joinTable = null, joinCondition = null; 
    if(joinPart)
    {
        const joinRegex = /^(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i; 
        const joinMatch = joinPart.match(joinRegex);
        if(!joinMatch)
        {
            throw new Error("Join clause does not match!");
        }

        joinTable = joinMatch[1].trim();
        joinCondition = 
        {
            left : joinMatch[2].trim(),
            right : joinMatch[3].trim()
        };
    }

    //student.name = John
    let whereClauses = [];
    if(whereClause){
        whereClauses = parseWhereClause(whereClause); 
    }
    console.log("wherecheck", whereClauses);

    return{
        fields : fields.split(',').map(field => field.trim()),
        table : table.trim(),
        whereClauses,
        joinTable,
        joinCondition
    }; 

}

function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        console.log("matched statement", match);
        if (match) {
            const [, field, operator, value] = match;
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });
}



module.exports = parseQuery;



















