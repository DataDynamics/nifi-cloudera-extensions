package org.apache.nifi.processors.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.hive.AuthenticationFailedException;
import org.apache.nifi.util.hive.ValidationResources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class HiveConfigurator {

    // for Cloudera
    public Collection<ValidationResult> validate(String configFiles, KerberosUserService kerberosUserService, AtomicReference<ValidationResources> validationResourceHolder, ComponentLog log) {
        List<ValidationResult> results = new ArrayList<>();
        Configuration hiveConfig = getConfigurationForValidation(validationResourceHolder, configFiles, log);
        boolean isSecurityEnabled = SecurityUtil.isSecurityEnabled(hiveConfig);
        if (isSecurityEnabled) {
            if (kerberosUserService == null) {
                results.add((new ValidationResult.Builder()).valid(false).subject(getClass().getSimpleName())
                        .explanation("Kerberos User Service must be provided when using a secure configuration").build());
            } else {
                results.add((new ValidationResult.Builder()).valid(true).build());
            }
        } else if (kerberosUserService != null) {
            log.warn("Configuration does not have security enabled, Kerberos User Service will be ignored");
        } else {
            results.add((new ValidationResult.Builder()).valid(true).build());
        }
        return results;
    }


    public Collection<ValidationResult> validate(String configFiles, String principal, String keyTab, String password,
                                                 AtomicReference<ValidationResources> validationResourceHolder, ComponentLog log) {

        final Configuration hiveConfig = getConfigurationForValidation(validationResourceHolder, configFiles, log);

        return new ArrayList<>(KerberosProperties.validatePrincipalWithKeytabOrPassword(this.getClass().getSimpleName(), hiveConfig, principal, keyTab, password, log));
    }

    public Configuration getConfigurationForValidation(AtomicReference<ValidationResources> validationResourceHolder, String configFiles, ComponentLog log) {
        ValidationResources resources = validationResourceHolder.get();

        // if no resources in the holder, or if the holder has different resources loaded,
        // then load the Configuration and set the new resources in the holder
        if (resources == null || !configFiles.equals(resources.getConfigResources())) {
            log.debug("Reloading validation resources");
            resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
            validationResourceHolder.set(resources);
        }

        return resources.getConfiguration();
    }

    public HiveConf getConfigurationFromFiles(final String configFiles) {
        final HiveConf hiveConfig = new HiveConf();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                hiveConfig.addResource(new Path(configFile.trim()));
            }
        }
        return hiveConfig;
    }

    public void preload(Configuration configuration) {
        try {
            FileSystem.get(configuration).close();
            UserGroupInformation.setConfiguration(configuration);
        } catch (IOException ioe) {
            // Suppress exception as future uses of this configuration will fail
        }
    }

    /**
     * Acquires a {@link UserGroupInformation} using the given {@link Configuration} and {@link KerberosUser}.
     *
     * @param hiveConfig   The Configuration to apply to the acquired UserGroupInformation
     * @param kerberosUser The KerberosUser to authenticate
     * @return A UserGroupInformation instance created using the Subject of the given KerberosUser
     * @throws AuthenticationFailedException if authentication fails
     * @see SecurityUtil#getUgiForKerberosUser(Configuration, KerberosUser)
     */
    public UserGroupInformation authenticate(final Configuration hiveConfig, KerberosUser kerberosUser) throws AuthenticationFailedException {
        try {
            return SecurityUtil.getUgiForKerberosUser(hiveConfig, kerberosUser);
        } catch (IOException ioe) {
            throw new AuthenticationFailedException("Kerberos Authentication for Hive failed", ioe);
        }
    }

    /**
     * As of Apache NiFi 1.5.0, due to changes made to
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}, which is used by this
     * class to authenticate a principal with Kerberos, Hive controller services no longer
     * attempt relogins explicitly.  For more information, please read the documentation for
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}.
     * <p/>
     * In previous versions of NiFi, a {@link org.apache.nifi.hadoop.KerberosTicketRenewer} was started by
     * {@link HiveConfigurator#authenticate(Configuration, String, String, long)} when the Hive
     * controller service was enabled.  The use of a separate thread to explicitly relogin could cause race conditions
     * with the implicit relogin attempts made by hadoop/Hive code on a thread that references the same
     * {@link UserGroupInformation} instance.  One of these threads could leave the
     * {@link javax.security.auth.Subject} in {@link UserGroupInformation} to be cleared or in an unexpected state
     * while the other thread is attempting to use the {@link javax.security.auth.Subject}, resulting in failed
     * authentication attempts that would leave the Hive controller service in an unrecoverable state.
     *
     * @see SecurityUtil#loginKerberos(Configuration, String, String)
     * @deprecated Use {@link SecurityUtil#getUgiForKerberosUser(Configuration, KerberosUser)}
     */
    @Deprecated
    public UserGroupInformation authenticate(final Configuration hiveConfig, String principal, String keyTab) throws AuthenticationFailedException {
        UserGroupInformation ugi;
        try {
            ugi = SecurityUtil.loginKerberos(hiveConfig, principal, keyTab);
        } catch (IOException ioe) {
            throw new AuthenticationFailedException("Kerberos Authentication for Hive failed", ioe);
        }
        return ugi;
    }

    /**
     * As of Apache NiFi 1.5.0, this method has been deprecated and is now a wrapper
     * method which invokes {@link HiveConfigurator#authenticate(Configuration, String, String)}. It will no longer start a
     * {@link org.apache.nifi.hadoop.KerberosTicketRenewer} to perform explicit relogins.
     *
     * @see HiveConfigurator#authenticate(Configuration, String, String)
     */
    @Deprecated
    public UserGroupInformation authenticate(final Configuration hiveConfig, String principal, String keyTab, long ticketRenewalPeriod) throws AuthenticationFailedException {
        return authenticate(hiveConfig, principal, keyTab);
    }
}
