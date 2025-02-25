package adql.translator;

/*
 * This file is part of ADQLLibrary.
 *
 * ADQLLibrary is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ADQLLibrary is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ADQLLibrary.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2017-2019 - Astronomisches Rechen Institut (ARI),
 *                       UDS/Centre de Données astronomiques de Strasbourg (CDS)
 */

import java.util.Iterator;

import adql.db.DBTable;
import adql.db.DBColumn;
import adql.db.DBType;
import adql.db.DBType.DBDatatype;
import adql.db.STCS.Region;
import adql.db.SearchColumnList;
import adql.db.exception.UnresolvedJoinException;
import adql.parser.ParseException;
import adql.parser.SQLServer_ADQLQueryFactory;
import adql.query.ADQLQuery;
import adql.query.ClauseSelect;
import adql.query.IdentifierField;
import adql.query.from.ADQLJoin;
import adql.query.from.ADQLTable;
import adql.query.from.FromContent;
import adql.query.operand.ADQLColumn;
import adql.query.operand.ADQLOperand;
import adql.query.operand.Concatenation;
import adql.query.operand.function.MathFunction;
import adql.query.operand.function.geometry.AreaFunction;
import adql.query.operand.function.geometry.BoxFunction;
import adql.query.operand.function.geometry.CentroidFunction;
import adql.query.operand.function.geometry.CircleFunction;
import adql.query.operand.function.geometry.ContainsFunction;
import adql.query.operand.function.geometry.DistanceFunction;
import adql.query.operand.function.geometry.ExtractCoord;
import adql.query.operand.function.geometry.ExtractCoordSys;
import adql.query.operand.function.geometry.IntersectsFunction;
import adql.query.operand.function.geometry.PointFunction;
import adql.query.operand.function.geometry.PolygonFunction;
import adql.query.operand.function.geometry.RegionFunction;

/**
 * MS SQL Server translator.
 *
 * <p><b>Important:</b>
 * 	This translator works correctly ONLY IF {@link SQLServer_ADQLQueryFactory}
 * 	has been used to create any ADQL query this translator is asked to
 * 	translate.
 * </p>
 *
 * TODO See how case sensitivity is supported by MS SQL Server and modify this translator accordingly.
 *
 * TODO Extend this class for each MS SQL Server extension supporting geometry and particularly
 *      {@link #translateGeometryFromDB(Object)}, {@link #translateGeometryToDB(adql.db.STCS.Region)} and all this other
 *      translate(...) functions for the ADQL's geometrical functions.
 *
 * TODO Check MS SQL Server datatypes (see {@link #convertTypeFromDB(int, String, String, String[])},
 *      {@link #convertTypeToDB(DBType)}).
 *
 * <p><i><b>Important note:</b>
 * 	Geometrical functions are not translated ; the translation returned for them
 * 	is their ADQL expression.
 * </i></p>
 *
 * @author Gr&eacute;gory Mantelet (ARI;CDS)
 * @version 1.5 (03/2019)
 * @since 1.4
 *
 * @see SQLServer_ADQLQueryFactory
 */
public class SQLServerTranslator extends JDBCTranslator {

	/** SQLServer will default to length 1 for types such as CHAR, VARCHAR,
	 * BINARY and VARBINARY if no length is defined. This static attribute is the default value set
	 * by this translator if no length is specified. */
	public static int DEFAULT_VARIABLE_LENGTH = 2048;

	/** <p>Indicate the case sensitivity to apply to each SQL identifier (only SCHEMA, TABLE and COLUMN).</p>
	 *
	 * <p><i>Note:
	 * 	In this implementation, this field is set by the constructor and never modified elsewhere.
	 * 	It would be better to never modify it after the construction in order to keep a certain consistency.
	 * </i></p>
	 */
	protected byte caseSensitivity = 0x00;

	/**
	 * Builds an SQLServerTranslator which always translates in SQL all identifiers (schema, table and column) in a case sensitive manner ;
	 * in other words, schema, table and column names will be surrounded by double quotes in the SQL translation.
	 */
	public SQLServerTranslator(){
		caseSensitivity = 0x0F;
	}

	/**
	 * Builds an SQLServerTranslator which always translates in SQL all identifiers (schema, table and column) in the specified case sensitivity ;
	 * in other words, schema, table and column names will all be surrounded or not by double quotes in the SQL translation.
	 *
	 * @param allCaseSensitive	<i>true</i> to translate all identifiers in a case sensitive manner (surrounded by double quotes), <i>false</i> for case insensitivity.
	 */
	public SQLServerTranslator(final boolean allCaseSensitive){
		caseSensitivity = allCaseSensitive ? (byte)0x0F : (byte)0x00;
	}

	/**
	 * Builds an SQLServerTranslator which will always translate in SQL identifiers with the defined case sensitivity.
	 *
	 * @param catalog	<i>true</i> to translate catalog names with double quotes (case sensitive in the DBMS), <i>false</i> otherwise.
	 * @param schema	<i>true</i> to translate schema names with double quotes (case sensitive in the DBMS), <i>false</i> otherwise.
	 * @param table		<i>true</i> to translate table names with double quotes (case sensitive in the DBMS), <i>false</i> otherwise.
	 * @param column	<i>true</i> to translate column names with double quotes (case sensitive in the DBMS), <i>false</i> otherwise.
	 */
	public SQLServerTranslator(final boolean catalog, final boolean schema, final boolean table, final boolean column){
		caseSensitivity = IdentifierField.CATALOG.setCaseSensitive(caseSensitivity, catalog);
		caseSensitivity = IdentifierField.SCHEMA.setCaseSensitive(caseSensitivity, schema);
		caseSensitivity = IdentifierField.TABLE.setCaseSensitive(caseSensitivity, table);
		caseSensitivity = IdentifierField.COLUMN.setCaseSensitive(caseSensitivity, column);
	}

	@Override
	public boolean isCaseSensitive(final IdentifierField field){
		return field == null ? false : field.isCaseSensitive(caseSensitivity);
	}

	/**
	 * For SQL Server, {@link #translate(ClauseSelect)} must be overridden for
	 * TOP/LIMIT handling. We must not add the LIMIT at the end of the query, it
	 * must go in the SELECT.
	 *
	 * @see #translate(ClauseSelect)
	 */
	@Override
	public String translate(ADQLQuery query) throws TranslationException{
		StringBuffer sql = new StringBuffer(translate(query.getSelect()));

		sql.append("\nFROM ").append(translate(query.getFrom()));

		if (!query.getWhere().isEmpty())
			sql.append('\n').append(translate(query.getWhere()));

		if (!query.getGroupBy().isEmpty())
			sql.append('\n').append(translate(query.getGroupBy()));

		if (!query.getHaving().isEmpty())
			sql.append('\n').append(translate(query.getHaving()));

		if (!query.getOrderBy().isEmpty())
			sql.append('\n').append(translate(query.getOrderBy()));

		return sql.toString();
	}

	@Override
	public String translate(ClauseSelect clause) throws TranslationException{
		String sql = null;

		for(int i = 0; i < clause.size(); i++){
			if (i == 0){
				sql = clause.getName() + (clause.distinctColumns() ? " DISTINCT" : "") + (clause.hasLimit() ? " TOP " + clause.getLimit() + " " : "");
			}else
				sql += " " + clause.getSeparator(i);

			sql += " " + translate(clause.get(i));
		}

		return sql;
	}

	@Override
	public String translate(Concatenation concat) throws TranslationException{
		StringBuffer translated = new StringBuffer();

		for(ADQLOperand op : concat){
			if (translated.length() > 0)
				translated.append(" + ");
			translated.append(translate(op));
		}

		return translated.toString();
	}

	@Override
	public String translate(final ADQLJoin join) throws TranslationException{
		StringBuffer sql = new StringBuffer(translate(join.getLeftTable()));

		sql.append(' ').append(join.getJoinType()).append(' ').append(translate(join.getRightTable())).append(' ');

		// CASE: NATURAL
		if (join.isNatural()){
			try{
				StringBuffer buf = new StringBuffer();

				// Find duplicated items between the two lists and translate them as ON conditions:
				DBColumn rightCol;
				SearchColumnList leftList = join.getLeftTable().getDBColumns();
				SearchColumnList rightList = join.getRightTable().getDBColumns();
				for(DBColumn leftCol : leftList){
					// search for at most one column with the same name in the RIGHT list
					// and throw an exception is there are several matches:
					rightCol = ADQLJoin.findAtMostOneColumn(leftCol.getADQLName(), (byte)0, rightList, false);
					// if there is one...
					if (rightCol != null){
						// ...check there is only one column with this name in the LEFT list,
						// and throw an exception if it is not the case:
						ADQLJoin.findExactlyOneColumn(leftCol.getADQLName(), (byte)0, leftList, true);
						// ...append the corresponding join condition:
						if (buf.length() > 0)
							buf.append(" AND ");
						buf.append(translate(generateJoinColumn(join.getLeftTable(), leftCol, new ADQLColumn(leftCol.getADQLName()))));
						buf.append("=");
						buf.append(translate(generateJoinColumn(join.getRightTable(), rightCol, new ADQLColumn(rightCol.getADQLName()))));
					}
				}

				sql.append("ON ").append(buf.toString());
			}catch(UnresolvedJoinException uje){
				throw new TranslationException("Impossible to resolve the NATURAL JOIN between " + join.getLeftTable().toADQL() + " and " + join.getRightTable().toADQL() + "!", uje);
			}
		}
		// CASE: USING
		else if (join.hasJoinedColumns()){
			try{
				StringBuffer buf = new StringBuffer();

				// For each columns of usingList, check there is in each list exactly one matching column, and then, translate it as ON condition:
				DBColumn leftCol, rightCol;
				ADQLColumn usingCol;
				SearchColumnList leftList = join.getLeftTable().getDBColumns();
				SearchColumnList rightList = join.getRightTable().getDBColumns();
				Iterator<ADQLColumn> itCols = join.getJoinedColumns();
				while(itCols.hasNext()){
					usingCol = itCols.next();
					// search for exactly one column with the same name in the LEFT list
					// and throw an exception if there is none, or if there are several matches:
					leftCol = ADQLJoin.findExactlyOneColumn(usingCol.getColumnName(), usingCol.getCaseSensitive(), leftList, true);
					// item in the RIGHT list:
					rightCol = ADQLJoin.findExactlyOneColumn(usingCol.getColumnName(), usingCol.getCaseSensitive(), rightList, false);
					// append the corresponding join condition:
					if (buf.length() > 0)
						buf.append(" AND ");
					buf.append(translate(generateJoinColumn(join.getLeftTable(), leftCol, usingCol)));
					buf.append("=");
					buf.append(translate(generateJoinColumn(join.getRightTable(), rightCol, usingCol)));
				}

				sql.append("ON ").append(buf.toString());
			}catch(UnresolvedJoinException uje){
				throw new TranslationException("Impossible to resolve the JOIN USING between " + join.getLeftTable().toADQL() + " and " + join.getRightTable().toADQL() + "!", uje);
			}
		}
		// DEFAULT CASE:
		else if (join.getJoinCondition() != null)
			sql.append(translate(join.getJoinCondition()));

		return sql.toString();
	}

    /**
     * Override the {@link JDBCTranslator} method to add the catalog name.
     *
     */
    @Override
    public String getQualifiedSchemaName(final DBTable table)
        {
        if (table == null || table.getDBSchemaName() == null)
            {
            return "";
            }
        StringBuffer buf = new StringBuffer();

        if (table.getDBCatalogName() != null)
            {
            appendIdentifier(
                buf,
                table.getDBCatalogName(),
                IdentifierField.CATALOG
                ).append('.');
            }

        appendIdentifier(
            buf,
            table.getDBSchemaName(),
            IdentifierField.SCHEMA
            );

            return buf.toString();
        }


        @Override
        public String getQualifiedTableName(final DBTable table)
        {
            if (table == null)
            {
                return "";
            }

            StringBuffer buf = new StringBuffer(getQualifiedSchemaName(table));

            // The exact string you want to match
            String targetString = "TAP_SCHEMA";

            // Check for an exact case-insensitive match
            if (getQualifiedSchemaName(table).contains(targetString)) {

                buf.append(".");

            } else {

                if (buf.length() > 0)
                {
                    buf.append(".dbo.");
                }
            }

            appendIdentifier(
              buf,
              table.getDBName(),
              IdentifierField.TABLE
            );

            return buf.toString();
            }


	/**
	 * Generate an ADQL column of the given table and with the given metadata.
	 *
	 * @param table			Parent table of the column to generate.
	 * @param colMeta		DB metadata of the column to generate.
	 * @param joinedColumn	The joined column (i.e. the ADQL column listed in a
	 *                   	USING) from which the generated column should
	 *                   	derive.
	 *                   	<i>If NULL, an {@link ADQLColumn} instance will be
	 *                   	created from scratch using the ADQL name of the
	 *                   	given DB metadata.</i>
	 *
	 * @return	The generated column.
	 */
	protected ADQLColumn generateJoinColumn(final FromContent table, final DBColumn colMeta, final ADQLColumn joinedColumn){
		ADQLColumn newCol = (joinedColumn == null ? new ADQLColumn(colMeta.getADQLName()) : new ADQLColumn(joinedColumn));
		if (table != null){
			if (table instanceof ADQLTable)
				newCol.setAdqlTable((ADQLTable)table);
			else
				newCol.setAdqlTable(new ADQLTable(table.getName()));
		}
		newCol.setDBLink(colMeta);
		return newCol;
	}

	@Override
	public String translate(final ExtractCoord extractCoord) throws TranslationException{
		return getDefaultADQLFunction(extractCoord);
	}

	@Override
	public String translate(final ExtractCoordSys extractCoordSys) throws TranslationException{
		return getDefaultADQLFunction(extractCoordSys);
	}

	@Override
	public String translate(final AreaFunction areaFunction) throws TranslationException{
		return getDefaultADQLFunction(areaFunction);
	}

	@Override
	public String translate(final CentroidFunction centroidFunction) throws TranslationException{
		return getDefaultADQLFunction(centroidFunction);
	}

	@Override
	public String translate(final DistanceFunction fct) throws TranslationException{
		return getDefaultADQLFunction(fct);
	}

	@Override
	public String translate(final ContainsFunction fct) throws TranslationException{
		return getDefaultADQLFunction(fct);
	}

	@Override
	public String translate(final IntersectsFunction fct) throws TranslationException{
		return getDefaultADQLFunction(fct);
	}

	@Override
	public String translate(final PointFunction point) throws TranslationException{
		return getDefaultADQLFunction(point);
	}

	@Override
	public String translate(final CircleFunction circle) throws TranslationException{
		return getDefaultADQLFunction(circle);
	}

	@Override
	public String translate(final BoxFunction box) throws TranslationException{
		return getDefaultADQLFunction(box);
	}

	@Override
	public String translate(final PolygonFunction polygon) throws TranslationException{
		return getDefaultADQLFunction(polygon);
	}

	@Override
	public String translate(final RegionFunction region) throws TranslationException{
		return getDefaultADQLFunction(region);
	}

	@Override
	public String translate(MathFunction fct) throws TranslationException{
		switch(fct.getType()){
			case TRUNCATE:
				// third argument to round nonzero means do a truncate
				return "round(convert(float, " + ((fct.getNbParameters() >= 2) ? (translate(fct.getParameter(0)) + ", " + translate(fct.getParameter(1))) : "") + "),1)";
			case MOD:
				return ((fct.getNbParameters() >= 2) ? ("convert(float, " + translate(fct.getParameter(0)) + ") % convert(float, " + translate(fct.getParameter(1)) + ")") : "");
			case ATAN2:
				return "ATN2(" + translate(fct.getParameter(0)) + ", " + translate(fct.getParameter(1)) + ")";

			/* In MS-SQLServer, the following functions returns a value of the
			 * same type as the given argument. However, ADQL requires that an
			 * SQLServer float (so a double in ADQL) is returned. So, in order
			 * to follow the ADQL standard, the given parameter must be
			 * converted into a float: */
			case ABS:
				return "abs(convert(float, " + translate(fct.getParameter(0)) + "))";
			case CEILING:
				return "ceiling(convert(float, " + translate(fct.getParameter(0)) + "))";
			case DEGREES:
				return "degrees(convert(float, " + translate(fct.getParameter(0)) + "))";
			case FLOOR:
				return "floor(convert(float, " + translate(fct.getParameter(0)) + "))";
			case RADIANS:
				return "radians(convert(float, " + translate(fct.getParameter(0)) + "))";
			case ROUND:
				return "round(convert(float, " + translate(fct.getParameter(0)) + ")" + ", " + translate(fct.getParameter(1)) + ")";

			default:
				return getDefaultADQLFunction(fct);
		}
	}

	@Override
	public DBType convertTypeFromDB(final int dbmsType, final String rawDbmsTypeName, String dbmsTypeName, final String[] params){
		// If no type is provided return VARCHAR:
		if (dbmsTypeName == null || dbmsTypeName.trim().length() == 0)
			return null;

		// Put the dbmsTypeName in lower case for the following comparisons:
		dbmsTypeName = dbmsTypeName.toLowerCase();

		// Extract the length parameter (always the first one):
		int lengthParam = DBType.NO_LENGTH;
		if (params != null && params.length > 0){
			try{
				lengthParam = Integer.parseInt(params[0]);
			}catch(NumberFormatException nfe){
			}
		}

		// SMALLINT
		if (dbmsTypeName.equals("smallint") || dbmsTypeName.equals("tinyint") || dbmsTypeName.equals("bit"))
			return new DBType(DBDatatype.SMALLINT);
		// INTEGER
		else if (dbmsTypeName.equals("int"))
			return new DBType(DBDatatype.INTEGER);
		// BIGINT
		else if (dbmsTypeName.equals("bigint") || dbmsTypeName.equals("unsigned bigint"))
			return new DBType(DBDatatype.BIGINT);
		// REAL (cf https://msdn.microsoft.com/fr-fr/library/ms173773(v=sql.120).aspx)
		else if (dbmsTypeName.equals("real") || (dbmsTypeName.equals("float") && lengthParam >= 1 && lengthParam <= 24))
			return new DBType(DBDatatype.REAL);
		// DOUBLE (cf https://msdn.microsoft.com/fr-fr/library/ms173773(v=sql.120).aspx)
		else if (dbmsTypeName.equals("float") || dbmsTypeName.equals("decimal") || dbmsTypeName.equals("numeric"))
			return new DBType(DBDatatype.DOUBLE);
		// BINARY
		else if (dbmsTypeName.equals("binary"))
			return new DBType(DBDatatype.BINARY, lengthParam);
		// VARBINARY
		else if (dbmsTypeName.equals("varbinary"))
			return new DBType(DBDatatype.VARBINARY, lengthParam);
		// CHAR
		else if (dbmsTypeName.equals("char") || dbmsTypeName.equals("nchar"))
			return new DBType(DBDatatype.CHAR, lengthParam);
		// VARCHAR
		else if (dbmsTypeName.equals("varchar") || dbmsTypeName.equals("nvarchar"))
			return new DBType(DBDatatype.VARCHAR, lengthParam);
		// BLOB
		else if (dbmsTypeName.equals("image"))
			return new DBType(DBDatatype.BLOB);
		// CLOB
		else if (dbmsTypeName.equals("text") || dbmsTypeName.equals("ntext"))
			return new DBType(DBDatatype.CLOB);
		// TIMESTAMP
		else if (dbmsTypeName.equals("timestamp") || dbmsTypeName.equals("datetime") || dbmsTypeName.equals("datetime2") || dbmsTypeName.equals("datetimeoffset") || dbmsTypeName.equals("smalldatetime") || dbmsTypeName.equals("time") || dbmsTypeName.equals("date") || dbmsTypeName.equals("date"))
			return new DBType(DBDatatype.TIMESTAMP);
		// Default:
		else
			return null;
	}

	@Override
	public String convertTypeToDB(final DBType type){
		if (type == null)
			return "varchar(" + DEFAULT_VARIABLE_LENGTH + ")";

		switch(type.type){

			case SMALLINT:
			case REAL:
			case BIGINT:
				return type.type.toString().toLowerCase();

                        case CHAR:
                        case VARCHAR:
                        case BINARY:
                        case VARBINARY:
				return type.type.toString() + "(" + (type.length > 0 ? type.length : DEFAULT_VARIABLE_LENGTH) + ")";

			case INTEGER:
				return "int";

			// (cf https://msdn.microsoft.com/fr-fr/library/ms173773(v=sql.120).aspx)
			case DOUBLE:
				return "float(53)";

			case TIMESTAMP:
				return "datetime";

			case BLOB:
				return "image";

			case CLOB:
				return "text";

			case POINT:
			case REGION:
			default:
				return "varchar";
		}
	}

	@Override
	public Region translateGeometryFromDB(final Object jdbcColValue) throws ParseException{
		throw new ParseException("Unsupported geometrical value! The value \"" + jdbcColValue + "\" can not be parsed as a region.");
	}

	@Override
	public Object translateGeometryToDB(final Region region) throws ParseException{
		throw new ParseException("Geometries can not be uploaded in the database in this implementation!");
	}

}
