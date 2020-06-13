package com.github.marcelooriano.solrj;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexarArquivo {
	
	
	private static final 	Logger		LOGGER				= LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private static final	String[]	SOLR_ZK_HOST 		= {"localhost:9983"};
	private static final	String		SOLR_COLLECTION		= "cbo",
										DATASET_DIR_PATH 	= "dataset/ESTRUTURA CBO",
										SEP					= ";";
										
	public static void main(String[] args) {
		
		
		try (CloudSolrClient solrClient = new CloudSolrClient.Builder(Arrays.asList(SOLR_ZK_HOST), Optional.of("/")).build()) {
			solrClient.setDefaultCollection(SOLR_COLLECTION);
			LOGGER.info(solrClient.ping().toString());
			File dataset_dir = new File(DATASET_DIR_PATH);
			if (!dataset_dir.isDirectory())
				throw new IOException("Tipo de arquivo inesperado: Necessário informar um diretório.");

//			----------------------------------------------------------
//			Trunca o índice antes de realizar nova carga.
			solrClient.deleteByQuery("*:*");
			solrClient.commit(true,true);
//			----------------------------------------------------------
			
			for (String filePath : dataset_dir.list())
				try {
					List<SolrInputDocument> inputDocuments = null;
					if (null != (inputDocuments = iterarArquivos(new File(DATASET_DIR_PATH + "/" + filePath))))
						solrClient.add(inputDocuments);
				} catch (IOException e) {
					LOGGER.error("Falha ao ler o arquivo " + filePath + ": ", e); 
				} catch (SolrServerException e) {
					LOGGER.error("Falha ao persistir registro: ", e);
				}
			solrClient.commit(true, true);
			solrClient.optimize(true,true);
		} catch (IOException | SolrServerException e) {
			LOGGER.error("Falha geral ao estabelecer conexão com o serviço do Solr: ", e);
		}

	}
	
	private static List<SolrInputDocument> iterarArquivos(File arquivoDados) throws IOException {
		List<SolrInputDocument> inputDocuments = new ArrayList<>();
		BufferedReader br = new BufferedReader(new FileReader(arquivoDados));
		String linha = null;
		Boolean isHeader = true;
		String[] header = null;
		while (null != (linha = br.readLine()))
			if (isHeader) {
				header = linha.split(SEP);
				isHeader = false;
			} else {
				SolrInputDocument inputDocument = new SolrInputDocument();
				String[] reg = linha.split(SEP);
				for (int pos = 0 ; pos < header.length ; pos++)
					inputDocument.addField(header[pos].toLowerCase(), reg[pos]);
				inputDocument.setField("origem", arquivoDados.getName().replaceAll(" ", "_"));
				inputDocuments.add(inputDocument);
			}
		br.close();
		return (inputDocuments.size() > 0) ? inputDocuments : null;
	}

}
