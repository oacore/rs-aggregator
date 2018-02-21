package uk.ac.core.resync.syncore;

import nl.knaw.dans.rs.aggregator.syncore.VerificationPolicy;
import nl.knaw.dans.rs.aggregator.syncore.VerificationStatus;

import static nl.knaw.dans.rs.aggregator.syncore.VerificationStatus.not_verified;
import static nl.knaw.dans.rs.aggregator.syncore.VerificationStatus.verification_failure;
import static nl.knaw.dans.rs.aggregator.syncore.VerificationStatus.verification_success;

/**
 * Created by mc26486 on 20/02/2018.
 */
public class COREVerificationPolicy implements VerificationPolicy {
    @Override
    public boolean continueVerification(VerificationStatus stHash, VerificationStatus stLastMod,
                                        VerificationStatus stSize) {
        return true;
    }

    @Override
    public boolean repeatDownload(VerificationStatus stHash, VerificationStatus stLastMod, VerificationStatus stSize) {
        if (stHash == verification_failure || stLastMod == verification_failure) {
            return true; // keep strict policy on last modification date.
        } else if (stHash == verification_success) {
            return false; // perfect.
        } else if ((stLastMod == verification_success && stSize == verification_success)) {
            return false; // will do if no hash is available.
        } else if (stHash == not_verified && stLastMod == not_verified && stSize == not_verified) {
            return false; // CORE:MC:will won't redownload as soon as the file exist for now
        }
        return true; // under all other conditions.
    }

    @Override
    public boolean isVerified(VerificationStatus stHash, VerificationStatus stLastMod, VerificationStatus stSize) {
        return true; //we don't run verification for now.
    }
}
