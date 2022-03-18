/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sensql;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static java.util.Objects.requireNonNull;

public class SenSQLModule
{
    private Connection localDB;
    private Driver driver;
   // private List<Expression> forwardClauses;
//    final private ConditionTrim localQueryCondition = this::checkCond;
//    private ConditionTrim forwardQueryCondition = this::
//    @FunctionalInterface
//    interface ConditionTrim {
//        void checkCond(Expression condition);
//    }

    public SenSQLModule()
    {
        System.out.println("Init SenSQLModule");
        try {
            System.out.println("attempting localDB connection");
            this.driver = new Driver();
            this.localDB = this.driver.connect("jdbc:postgresql://db.sece.io/geonaming?"
                        + "user=geo_api"
                        + "&password=g3QMmAsv"
                        + "&sslmode=require", new Properties());
            System.out.println("\nlocalDB connection: ");
            System.out.println(this.localDB);
            java.sql.Statement s = this.localDB.createStatement();
            s.executeUpdate("set session.uid='R2wzx2ovqEeBSjcRxgPZ9ZNT2j33';");
            s.close();
        }
        catch (SQLException e) {
            System.out.println("FAILED TO CONNECT TO LOCALDB");
            e.printStackTrace();
        }
    }

    public QueryPreparer.PreparedQuery rewrite(QuerySpecification originalBody, QueryPreparer queryPreparer, Session session, WarningCollector warningCollector)
    {
        // get relation name from 'from' clause
        if (originalBody.from.isPresent()) {
            originalBody.from = Optional.of(processFrom(originalBody.from.get()));
        }

        String from = ((Table) originalBody.from.get()).getName().toString();

        // get location from 'where' clause
        List<String> whereList = new LinkedList<>();

        Statement localQuery = null;

        if (originalBody.where.isPresent()) {
            // make copy for forward query
            localQuery = queryPreparer.sqlParser.createStatement("select nodes.id from\n" +
                    "nodes, feature, shape where\n" +
                    "st_intersects(shape.geometries, nodes.service_region) and shape.id = feature.shape and "
                    + originalBody.where.get());
            // process where clause for presto query
//            System.out.println("original where: " + originalBody.where);
//            System.out.println("-- beginning recursion --");
            originalBody.where = Optional.of(processWhere(originalBody.where.get(), this::checkCond));
//            System.out.println("-- end recursion --");
            if (originalBody.where.get() instanceof BooleanLiteral) {
                originalBody.where = Optional.empty();
            }
            System.out.println("modified where: " + originalBody.where);
        }
        // now start buildling the localdb query
//        System.out.println("original localQuery: " + ((Query) localQuery).queryBody.toString());
        String forwardString = "";
        if (localQuery != null) {
            QuerySpecification forwardBody = ((QuerySpecification) ((((Query) localQuery).getQueryBody())));

//            System.out.println("-- beginning recursion --");
            forwardBody.where = Optional.of(processWhere(forwardBody.where.get(), this::checkCondBackend));
//            System.out.println("-- end recursion --");
            forwardString = "select " + forwardBody.select.getSelectItems().get(0)
                    + " from " + ((Table) ((Join) ((Join) forwardBody.from.get()).getLeft()).getLeft()).getName() + ", "
                    + ((Table) ((Join) ((Join) forwardBody.from.get()).getLeft()).getRight()).getName() + ", "
                    + ((Table) ((Join) forwardBody.from.get()).getRight()).getName()
                    + " where " + forwardBody.where.get() + " group by " + forwardBody.select.getSelectItems().get(0);

            // exec localdb query and build wherelist from results
            try {
                System.out.println("executing localDB query: [" + forwardString + "]");
                java.sql.Statement s = this.localDB.createStatement();
                ResultSet rs = s.executeQuery(forwardString);
                while (rs.next()) {
//                    System.out.println("row: " + rs.getString(1));
                    whereList.add(rs.getString(1));
                }
            }
            catch (SQLException sqle) {
                sqle.printStackTrace();
            }
        }
//        if (whereList.size() == 0) {
//            whereList.add("test3");
//        }

        System.out.print("catalogs from localDB: " + whereList);
        // default location = 'Morningside Heights' if none found

//        System.out.println("forwardList: " + this.forwardClauses);
//        QuerySpecification localBody = ((QuerySpecification) ((((Query) localQuery).getQueryBody())));
//        System.out.println(localBody);

        // create 'with' clause
        // With{[WithQuery{Union}]}
        WithQuery[] withquery = new WithQuery[1];
        List<Relation> unionRelations = new LinkedList<>();
        for (String s : whereList) {
            // add select * from [id] for every id returned from localDB
            unionRelations.add(((Query) queryPreparer.sqlParser.createStatement(
                    "select * from " + s + ".public." + from,
                    createParsingOptions(session, warningCollector))).queryBody);
        }

        if (unionRelations.size() > 1) {
            Union union = new Union(unionRelations, Optional.of(false));
            withquery[0] = new WithQuery(new Identifier(from), new Query(Optional.empty(), union, Optional.empty(), Optional.empty(), Optional.empty()), Optional.empty());
        }
        else {
            withquery[0] = new WithQuery(new Identifier(from), new Query(Optional.empty(), (QueryBody) unionRelations.get(0), Optional.empty(), Optional.empty(), Optional.empty()), Optional.empty());
        }
        With with = new With(false, Arrays.asList(withquery));

        Query newQuery = new Query(Optional.of(with), originalBody, Optional.empty(), Optional.empty(), Optional.empty());

//        System.out.println(newQuery.queryBody);

        return queryPreparer.prepareQuery(session, newQuery, warningCollector);
    }

    private Table processFrom(Relation from)
    {
        // Right now just supports two tables in original query
        Table result = new Table(QualifiedName.of(""));
        try {
            result = (Table) from;

            return result;
        }
        catch (ClassCastException e) {
        }
        try {
            Join join = (Join) from;
            if (((Table) join.getRight()).getName().toString().equals("feature")) {
                result = new Table(QualifiedName.of(((Table) join.getLeft()).getName().toString()));
            }
            if (((Table) join.getLeft()).getName().toString().equals("feature")) {
                result = new Table(QualifiedName.of(((Table) join.getRight()).getName().toString()));
            }
        }
        catch (ClassCastException e) {
        }
        return result;
    }

    private Expression processWhere(Expression expression, Function<Expression, Boolean> condition)
    {
//        System.out.println(expression.toString());

        try {
            LogicalBinaryExpression lbExpression = (LogicalBinaryExpression) expression;

            try {
                lbExpression.right = processWhere(lbExpression.right, condition);
            }
            catch (ClassCastException e) {
            }

            try {
                lbExpression.left = processWhere(lbExpression.left, condition);
            }
            catch (ClassCastException e) {
            }
            // removing both left & right expressions
            if (lbExpression.left instanceof BooleanLiteral && lbExpression.right instanceof BooleanLiteral) {
                return new BooleanLiteral(true);
            }
            // removing only left
            else if (lbExpression.left instanceof BooleanLiteral) {
                return lbExpression.right;
            }
            // removing only right
            else if (lbExpression.right instanceof BooleanLiteral) {
                return lbExpression.left;
            }
            // removing neither
            else {
                boolean left = condition.apply(lbExpression.left);
                boolean right = condition.apply(lbExpression.right);
                if (left && right) {
                    if (lbExpression.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
                        return new BooleanLiteral(true);
                    }
                    else {
                        return new BooleanLiteral(false);
                    }
                }
                if (left) {
                    return lbExpression.right;
                }
                if (right) {
                    return lbExpression.left;
                }
                return lbExpression;
            }
        }
        catch (ClassCastException e) {
        }
        try {
            NotExpression nExpression = (NotExpression) expression;
            if (condition.apply(nExpression)) {
                return new BooleanLiteral(true);
            }
            else {
                return expression;
            }
        }
        catch (ClassCastException e) {
        }
        if (condition.apply(expression)) {
            return new BooleanLiteral(true);
        }
        return expression;
    }

    private boolean checkCond(Expression condition)
    {
        try {
            if (!(condition instanceof LogicalBinaryExpression)) {
//                System.out.println("checking condition: " + condition.toString());
//                System.out.println("checking for 'feature': " + condition.toString());
                // This currently also removes st_*() clauses, intentional while we aren't yet geoquerying nodes
                if (condition.toString().contains("feature") || condition.toString().contains("shape")
                        || condition.toString().contains("node")) {
//                    System.out.println("found");
//                    this.forwardClauses.add(Condition);
//                    if (!(condition instanceof NotExpression)) {
//                        whereList.add(((StringLiteral) (((ComparisonExpression) condition).getRight())).getValue());
//                    }
                    return true;
                }
            }
        }
        catch (ClassCastException e) {
        }
        return false;
    }

    private boolean checkCondBackend(Expression condition)
    {
        try {
            if (!(condition instanceof LogicalBinaryExpression)) {
//                System.out.println("checking condition: " + condition.toString());
//                if (condition.toString().contains("measurements")) {
//                    System.out.println("contains 'measurements'");
//                }
                if ((!condition.toString().contains("feature") && !condition.toString().contains("shape"))
                        || condition.toString().contains("measurements")/* || condition.toString().contains("st_")*/) {
//                    this.forwardClauses.add(Condition);
                    return true;
                }
            }
        }
        catch (ClassCastException e) {
        }
        return false;
    }

    private Statement rewrite(Statement node, SqlParser sqlParser, Session session, WarningCollector warningCollector)
    {
        return (Statement) new Visitor(session, sqlParser, warningCollector).process(node, null);
    }

    // attempt to write rewrite as a visitor operation...
    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final SqlParser parser;
        private final WarningCollector warningCollector;

        public Visitor(
                Session session,
                SqlParser parser,
                WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.parser = parser;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitQuery(Query node, Void context)
                throws SemanticException
        {
//            System.out.println("visit query");
            for (Node child : node.getChildren()) {
                process(child);
            }
            return node;
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        @Override
        protected Relation visitRelation(Relation node, Void context)
        {
//            System.out.println("visit relation: " + node.toString());
            return node;
        }

        @Override
        protected QueryBody visitQueryBody(QueryBody node, Void context)
        {
//            System.out.println("visit query body");
            return node;
        }

        @Override
        protected QuerySpecification visitQuerySpecification(QuerySpecification node, Void context)
        {
//            System.out.println("visit query specification");
            return node;
        }
    }
}
