//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.qst.table.TableSpec;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * This is the main public entrypoint for converting a SQL query into {@link TableSpec}.
 */
public final class SqlAdapter {

    private static final Logger log = LoggerFactory.getLogger(SqlAdapter.class);

    /**
     * Parses the {@code sql} query into a {@link TableSpec}.
     *
     * <p>
     * Note: only {@link ScopeStaticImpl} is supported right now.
     *
     * @param sql the sql
     * @param scope the scope
     * @return the table spec
     */
    public static TableSpec parseSql(
            String sql,
            Scope scope) {
        // 0: Configuration
        // SQLTODO(parse-sql-configuration)
        //
        // Allow for customization of calcite parsing details. Would this mean that calcite should / would become part
        // of the public API, or would it be kept as an implementation detail?
        //
        // For example, should we allow the user to configure lexing configuration? #parserConfig() /
        // #calciteConnectionConfig()

        // 1: Parse into AST
        final SqlNode node = parse(sql);

        // 2: Validate AST
        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final CatalogReader catalogReader = reader(typeFactory, scope);
        final SqlValidator validator = validator(typeFactory, catalogReader);
        final SqlNode validNode = validator.validate(node);

        // 3: Convert into relational node
        final RelNode relNode = convert(typeFactory, catalogReader, validator, validNode);
        if (log.isDebugEnabled()) {
            log.debug(RelOptUtil.dumpPlan(
                    "[Logical plan]",
                    relNode,
                    SqlExplainFormat.TEXT,
                    SqlExplainLevel.ALL_ATTRIBUTES));
        }

        // 4: Relational optimization
        // SQLTODO(rel-node-optimization)
        //
        // Use calcite for optimization. Simple optimizations are filter / predicate pushdowns (for example, any filters
        // exclusively on the LHS or RHS of a join). More advanced optimizations may be possible by attaching statistics
        // or hints about the underlying structure of the source data.

        // 5. Convert into QST
        // SQLTODO(qst-convert-optimization)
        //
        // The conversion process from RelNode to QST can be optimized if additional hints are added to the source data,
        // or additional relational details about the data are known.
        //
        // For example, if we know that all Ids in a column are unique, either because the user has hinted as much, or
        // we know the Ids are unique because they are a "last_by" construction, we may be able to use a natural_join
        // instead of a join for an INNER JOIN conversion.

        // SQLTODO(table-headers-out)
        // In addition to parsing and creating the execution plan TableSpec, it would be reasonable to create and
        // return the expected TableHeader, or even Map<TableSpec, TableHeader> for the full dag. This might aid in
        // executing TableSpec; or at least, provides a way to check if the engine output matches what SQL expects the
        // output type to be.
        return RelNodeAdapterNamed.of(SqlRootContext.of(relNode, scope), relNode);

        // 6. QST optimization
        // SQLTODO(qst-optimization)
        // There are QST optimizations that are orthogonal to SQL, but none-the-less may be useful and can be guided
        // based on the types of unoptimized structures that this implementation may make.
        //
        // Most immediately, this may be optimize away some of the "excessive" implementation layers that the SQL
        // adapting layer is adding. (Note: these "excessive" layers are implementation details that aid in ease of
        // implementing and maintaining said adapting.)
    }

    private static SqlNode parse(String sql) {
        final SqlParser parser = SqlParser.create(sql, LexConfig.withLexConfig(SqlParser.config()));
        final SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch (org.apache.calcite.sql.parser.SqlParseException e) {
            throw new SqlParseException(e);
        }
        return sqlNode;
    }

    private static CatalogReader reader(
            RelDataTypeFactory typeFactory,
            Scope scope) {
        // SQLTODO(catalog-reader-implementation)
        //
        // The current implementation assumes that the user will pass in all the headers they care about. When executing
        // a SQL query, this means that either a) the user is explicit about the context they care about or b) all
        // possible headers are collected. a) is not very user friendly and b) is inefficient.
        //
        // A potentially better model would be an interface where the caller could produce the TableHeaders on demand
        // instead of requiring them all up-front. This would require implementing / wrapping
        // org.apache.calcite.prepare.Prepare.CatalogReader which is possible, but non-trivial. There may also be
        // boundary-crossing concerns wrt python.
        if (!(scope instanceof ScopeStaticImpl)) {
            throw new IllegalArgumentException(
                    "SQLTODO(catalog-reader-implementation): only ScopeStaticImpl is supported");
        }
        final CalciteSchema schema = CalciteSchema.createRootSchema(true);
        for (TableInformation info : ((ScopeStaticImpl) scope).tables()) {
            if (info.qualifiedName().size() != 1) {
                throw new UnsupportedOperationException("Only expecting qualified names with one part");
            }
            schema.add(info.qualifiedName().get(0), new DeephavenTable(TypeAdapter.of(info.header(), typeFactory)));
        }
        final Properties props = new Properties();
        LexConfig.setLexProperties(props);
        return new CalciteCatalogReader(schema, List.of(), typeFactory, new CalciteConnectionConfigImpl(props));
    }

    private static SqlValidator validator(
            RelDataTypeFactory typeFactory,
            CatalogReader catalogReader) {
        // DH sorts nulls first when ascending and nulls last when descending; when the user / SQL string doesn't
        // specify NULLS FIRST nor NULLS LAST, we should assume the DH-safe defaults. This aligns with
        // NullCollation.LOW configuration.
        final SqlValidator.Config config = SqlValidator.Config.DEFAULT.withDefaultNullCollation(NullCollation.LOW);
        return SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(), catalogReader, typeFactory, config);
    }

    private static RelNode convert(
            RelDataTypeFactory typeFactory,
            CatalogReader catalogReader,
            SqlValidator validator,
            SqlNode validNode) {
        final RelOptCluster cluster = newCluster(typeFactory);
        final SqlToRelConverter converter = new SqlToRelConverter(
                ViewExpanders.simpleContext(cluster),
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());
        return converter.convertQuery(validNode, false, true).rel;
    }

    private static RelOptCluster newCluster(RelDataTypeFactory factory) {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, new RexBuilder(factory));
    }
}
