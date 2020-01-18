package com.org.dao;

import java.util.Optional;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.transaction.Transactional;
import org.springframework.stereotype.Repository;
import com.org.entity.EmployeeEntity;
import com.org.repository.EmployeeRepository;

@Transactional
@Repository
public class EmployeeDao implements EmployeeRepository {

	@PersistenceContext
	private EntityManager entityManager;

	@Override
	public <S extends EmployeeEntity> S save(S entity) {
		// TODO Auto-generated method stub
		return entityManager.merge(entity);
	}

	@Override
	public Optional<EmployeeEntity> findById(Long id) {
		// TODO Auto-generated method stub
		return Optional.of(entityManager.find(EmployeeEntity.class, id));
	}

	@Override
	public Optional<EmployeeEntity> findByEmail(String email) {
		// TODO Auto-generated method stub
		Query query = entityManager.createQuery("FROM EmployeeEntity e WHERE e.email = :email", EmployeeEntity.class);
		query.setParameter("email", email);

		try {
			return Optional.of((EmployeeEntity) query.getSingleResult());

		} catch (Exception e) {
			return null;
			
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterable<EmployeeEntity> findAll() {
		// TODO Auto-generated method stub
		return entityManager.createQuery("SELECT e FROM EmployeeEntity e").getResultList();
	}

	@Override
	public void deleteById(Long id) {
		// TODO Auto-generated method stub
		entityManager.remove(id);
	}

	@Override
	public <S extends EmployeeEntity> Iterable<S> saveAll(Iterable<S> entities) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean existsById(Long id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterable<EmployeeEntity> findAllById(Iterable<Long> ids) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long count() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void delete(EmployeeEntity entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteAll(Iterable<? extends EmployeeEntity> entities) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteAll() {
		// TODO Auto-generated method stub

	}
}
