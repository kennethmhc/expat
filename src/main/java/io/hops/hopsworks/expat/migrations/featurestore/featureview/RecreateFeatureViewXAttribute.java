package io.hops.hopsworks.expat.migrations.featurestore.featureview;

import com.google.common.collect.Sets;
import io.hops.hopsworks.common.featurestore.xattr.dto.FeaturestoreXAttrsConstants;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrException;
import io.hops.hopsworks.expat.migrations.projects.util.XAttrHelper;

import javax.xml.bind.JAXBException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Set;

public class RecreateFeatureViewXAttribute extends CreateFeatureViewFromTrainingDataset {

  private final static String GET_ALL_FEATURE_VIEWS =
      "SELECT a.id, a.feature_store_id, a.name, a.version, b.project_id, " +
          "d.uid, c.projectname, d.username, d.email, a.created, a.creator, a.description " +
          "FROM feature_view AS a " +
          "INNER JOIN feature_store AS b ON a.feature_store_id = b.id " +
          "INNER JOIN project AS c ON b.project_id = c.id " +
          "INNER JOIN users AS d on a.creator = d.uid ";

  private final static Set<String> targetFvs = Sets.newHashSet(
      "loan_writeoff_credit_card_2",
      "auto_loan_writeoff_data_5",
      "dynamic_dataset_2",
      "rv_preapproval_limit_1",
      "auto_loan_writeoff_data_8",
      "waterfall_auto_test_1",
      "test_1",
      "training_loan_origination_cc_2",
      "auto_loan_writeoff_data_7",
      "loan_writeoff_credit_card_6",
      "loan_writeoff_credit_card_7",
      "waterfall_auto_7",
      "personal_loan_underwriting_data_5",
      "rv_pre_approval_td_filter_5",
      "auto_loan_writeoff_data_9",
      "waterfall_auto_5",
      "loan_writeoff_loc_2",
      "rv_single_collateral__1",
      "dynamic_dataset_3",
      "rv_pre_approval_td_filter_2",
      "waterfall_auto_1",
      "personal_loan_underwriting_data_2",
      "loan_writeoff_credit_card_1",
      "auto_loan_writeoff_data_2",
      "personal_loan_underwriting_data_3",
      "rv_pre_approval_td_1",
      "dynamic_dataset_5",
      "personal_loan_underwriting_data_7",
      "dynamic_dataset_1",
      "auto_loan_writeoff_data_3",
      "auto_loan_writeoff_data_4",
      "waterfall_auto_3",
      "personal_loan_underwriting_data_4",
      "rv_pre_approval_limit_1",
      "rv_pre_approval_td_filter_4",
      "erm_model_1",
      "waterfall_auto_4",
      "loan_writeoff_credit_card_8",
      "rv_pre_approval_td_filter_1",
      "loan_writeoff_credit_card_5",
      "dynamic_dataset_4",
      "auto_loan_writeoff_data_6",
      "personal_loan_underwriting_data_6",
      "loan_origination_training_1",
      "loan_writeoff_credit_card_4",
      "waterfall_auto_8",
      "tour_training_dataset_test_1",
      "rv_pre_approval_td_filter_3",
      "training_loan_origination_cc_1",
      "loan_writeoff_credit_card_3",
      "auto_loan_writeoff_data_1",
      "waterfall_auto_2",
      "waterfall_auto_6",
      "rv_test_1",
      "rv_test_numeric_filter_1",
      "personal_loan_underwriting_data_1",
      "loan_writeoff_loc_1"
  );

  public RecreateFeatureViewXAttribute() throws JAXBException {
    super();
  }

  @Override
  public void runRollback() throws RollbackException {
    try {
      connection.setAutoCommit(false);
      PreparedStatement getFeatureViewsStatement = connection.prepareStatement(GET_ALL_FEATURE_VIEWS);
      ResultSet featureViews = getFeatureViewsStatement.executeQuery();
      Integer n = 0;
      while (featureViews.next()) {
        String name = featureViews.getString("name");
        Integer version = featureViews.getInt("version");
        String projectName = featureViews.getString("projectname");
        if (!targetFvs.contains(name + "_" + version)) {
          continue;
        }
        n += 1;
        if (!dryRun) {
          XAttrHelper.upsertProvXAttr(dfso, getFeatureViewFullPath(projectName, name, version).toString(),
              FeaturestoreXAttrsConstants.FEATURESTORE, new byte[0]);
        } else {
          LOGGER.info(
              String.format("Content of XAtrribute at %s will be removed.",
                  getFeatureViewFullPath(projectName, name, version)
              )
          );
        }
      }
      getFeatureViewsStatement.close();
      connection.commit();
      connection.setAutoCommit(true);
      LOGGER.info(n + " feature view xattributes have been updated.");
    } catch (XAttrException | SQLException e) {
      throw new RollbackException("Rollback failed. Cannot commit.", e);
    } finally {
      super.close();
    }

  }

  @Override
  public void runMigration() throws MigrationException {
    try {
      connection.setAutoCommit(false);
      PreparedStatement getFeatureViewsStatement = connection.prepareStatement(GET_ALL_FEATURE_VIEWS);
      ResultSet featureViews = getFeatureViewsStatement.executeQuery();
      Integer n = 0;
      while (featureViews.next()) {
        String name = featureViews.getString("name");
        Integer version = featureViews.getInt("version");
        if (!targetFvs.contains(name + "_" + version)) {
          continue;
        }
        Integer featurestoreId = featureViews.getInt("feature_store_id");
        Timestamp created = featureViews.getTimestamp("created");
        String description = featureViews.getString("description");

        Integer trainingDatasetId = featureViews.getInt("id");
        String projectName = featureViews.getString("projectname");
        String email = featureViews.getString("email");

        Integer[] features = getArray("training_dataset_feature", "feature_view_id", trainingDatasetId);
        n += 1;
        byte[] attr = setXAttr(featurestoreId, getFeatureViewFullPath(projectName, name, version).toString(),
            description, created, email, features);
        if (dryRun && n < 5) {
          LOGGER.info("Feature view: " + name + "\t" + new String(attr));
        }
      }
      getFeatureViewsStatement.close();
      connection.commit();
      connection.setAutoCommit(true);
      LOGGER.info(n + " feature view xattributes have been updated.");
    } catch (SQLException e) {
      throw new MigrationException("Migration failed. Cannot commit.", e);
    } finally {
      super.close();
    }
  }
}
