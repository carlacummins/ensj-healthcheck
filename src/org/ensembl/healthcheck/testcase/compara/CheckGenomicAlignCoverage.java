/*
 * Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
 * Copyright [2016-2019] EMBL-European Bioinformatics Institute
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.ensembl.healthcheck.testcase.compara;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.ensembl.healthcheck.DatabaseRegistryEntry;
import org.ensembl.healthcheck.ReportManager;
import org.ensembl.healthcheck.Team;
import org.ensembl.healthcheck.testcase.compara.AbstractComparaTestCase;
import org.ensembl.healthcheck.util.DBUtils;

/**
 * An EnsEMBL Healthcheck test case that checks that the coverage for a
 * method_link_species_set matches the coverage recorded in the  mlss_tag
 * table
 */

public class CheckGenomicAlignCoverage extends AbstractComparaTestCase {

	/**
	 * Create a CheckGenomicAlignCoverage that applies to a specific set of
	 * databases.
	 */
	public CheckGenomicAlignCoverage() {
		setDescription("Check the actual coverage for a method_link_species_set matches the coverage tag");
		setTeamResponsible(Team.COMPARA);
	}

	/**
	 * Run the test.
	 * 
	 * @param dbre
	 *            The database to use.
	 * @return true if the test passed.
	 * 
	 */
	public boolean run(DatabaseRegistryEntry dbre) {

		boolean result = true;

		Connection con = dbre.getConnection();
		String comparaDbName = (con == null) ? "no_database" : DBUtils.getShortDatabaseName(con);

		/**
		 * Get all method_link_species_set_ids for LASTZ_NET (method_link_id = 16)
		 */
		String[] method_link_species_set_ids = DBUtils
				.getColumnValues(con,
						"SELECT method_link_species_set_id FROM method_link_species_set WHERE method_link_id = 16");

		if (method_link_species_set_ids.length > 0) {

			for (String mlss_id : method_link_species_set_ids) {
				/**
				SELECT SUM(IF((is_reference = 1 AND tag_coverage = genomic_align_coverage) OR (is_reference = 0 AND tag_coverage <= genomic_align_coverage), 1, 0)) AS coverage_ok FROM (
					SELECT g.name, d.genome_db_id, x.tag_coverage, SUM(ga.dnafrag_end-ga.dnafrag_start+1) AS genomic_align_coverage, IF(x.ref_status = 'ref', 1, 0) AS is_reference 
					FROM genomic_align ga JOIN dnafrag d USING(dnafrag_id) JOIN genome_db g USING(genome_db_id) JOIN (
						SELECT LEFT(tag, 3) AS ref_status, GROUP_CONCAT(IF(tag LIKE '%species', value, NULL)) AS species_name, GROUP_CONCAT(IF(tag LIKE '%coverage', value, NULL)) AS tag_coverage 
						FROM method_link_species_set_tag WHERE (tag LIKE '%species' OR tag LIKE '%coverage') AND method_link_species_set_id = 1234 GROUP BY LEFT(tag, 3)
					) x ON x.species_name = g.name WHERE ga.method_link_species_set_id = 1234 GROUP BY g.name
				) y;
				*/
				String tag_coverage_sql = "SELECT LEFT(tag, 3) AS ref_status, GROUP_CONCAT(IF(tag LIKE '%species', value, NULL)) AS species_name, " +
					"GROUP_CONCAT(IF(tag LIKE '%coverage', value, NULL)) AS tag_coverage " +
					"FROM method_link_species_set_tag WHERE (tag LIKE '%species' OR tag LIKE '%genome_coverage') " +
					"AND method_link_species_set_id = " + mlss_id + " GROUP BY LEFT(tag, 3)";
				String genomic_coverage_sql = "SELECT g.name, d.genome_db_id, x.tag_coverage, SUM(ga.dnafrag_end-ga.dnafrag_start+1) AS genomic_align_coverage, " +
					"IF(x.ref_status = 'ref', 1, 0) AS is_reference " +
					"FROM genomic_align ga JOIN dnafrag d USING(dnafrag_id) JOIN genome_db g USING(genome_db_id) JOIN (" + tag_coverage_sql + 
					") x ON x.species_name = g.name WHERE ga.method_link_species_set_id = " + mlss_id + " GROUP BY g.name";
				String summary_sql = "SELECT SUM(IF((is_reference = 1 AND tag_coverage = genomic_align_coverage) " +
					"OR (is_reference = 0 AND tag_coverage <= genomic_align_coverage), 1, 0)) AS coverage_ok " +
					"FROM (" + genomic_coverage_sql + ") y";
				
				String coverage_ok = DBUtils.getRowColumnValue(con, summary_sql);
				if ( !coverage_ok.equals("2") ) {
					ReportManager.problem(this, con, "FAILED genomic_align coverage does not match method_link_species_set_tag coverage");
					ReportManager.problem(this, con, "FAILURE DETAILS: Alignment coverage for method_link_species_set_id " + mlss_id + " is inconsistent. "+coverage_ok+"/2 coverage values correct.");
					ReportManager.problem(this, con, "USEFUL SQL: " + summary_sql);
					result = false;
				}
			}
		}

		return result;
	}
	
	// public static String getSingleIntValue(Connection con, String sql) {
	// 
	// 	List<String> results = getSqlTemplate(con).queryForDefaultObjectList(sql, String.class);
	// 	String first_elem = CollectionUtils.getFirstElement(results, StringUtils.EMPTY);
	// 	int first_elem_int = Integer.parseInt(first_elem);
	// 	return first_elem_int;
	// 
	// }

} // CheckGenomicAlignCoverage
