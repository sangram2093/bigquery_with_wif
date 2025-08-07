package org.example;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.*;
import java.util.*;

public class PemUtils {

    public static KeyManagerFactory createKeyManagerFactory(String clientPemContent) throws Exception {
        String privateKey = extractSection(clientPemContent, "PRIVATE KEY");
        String certificate = extractSection(clientPemContent, "CERTIFICATE");

        byte[] decodedKey = Base64.getDecoder().decode(privateKey);
        byte[] decodedCert = Base64.getDecoder().decode(certificate);

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decodedKey);
        PrivateKey privKey = keyFactory.generatePrivate(keySpec);

        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        InputStream certStream = new ByteArrayInputStream(decodedCert);
        Certificate cert = certFactory.generateCertificate(certStream);

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("client", privKey, "password".toCharArray(), new Certificate[]{cert});

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(keyStore, "password".toCharArray());
        return kmf;
    }

    public static TrustManagerFactory createTrustManagerFactory(String caChainContent) throws Exception {
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        InputStream caInputStream = new ByteArrayInputStream(caChainContent.getBytes(StandardCharsets.UTF_8));

        Collection<? extends Certificate> caCerts = certFactory.generateCertificates(caInputStream);
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null);

        int i = 0;
        for (Certificate cert : caCerts) {
            trustStore.setCertificateEntry("ca" + i, cert);
            i++;
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(trustStore);
        return tmf;
    }

    private static String extractSection(String pem, String sectionName) {
        String start = "-----BEGIN " + sectionName + "-----";
        String end = "-----END " + sectionName + "-----";

        int startIndex = pem.indexOf(start);
        int endIndex = pem.indexOf(end);

        if (startIndex == -1 || endIndex == -1)
            throw new IllegalArgumentException(sectionName + " not found in PEM");

        return pem.substring(startIndex + start.length(), endIndex).replaceAll("\\s+", "");
    }
}
