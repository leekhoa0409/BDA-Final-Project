document.addEventListener('DOMContentLoaded', () => {
    const hourSelect = document.getElementById('hour');
    const form = document.getElementById('prediction-form');
    const submitBtn = document.getElementById('submit-btn');
    const modelStatus = document.getElementById('model-status');
    const modelTag = document.getElementById('model-tag');
    
    // Populate hour select
    for (let i = 0; i < 24; i++) {
        const option = document.createElement('option');
        option.value = i;
        option.textContent = i === 0 ? '12 AM' : (i < 12 ? `${i} AM` : (i === 12 ? '12 PM' : `${i - 12} PM`));
        if (i === new Date().getHours()) option.selected = true;
        hourSelect.appendChild(option);
    }

    // Set current month and day
    const now = new Date();
    document.getElementById('month').value = now.getMonth() + 1;
    let dayForValue = now.getDay(); // 0 is Sunday, 1 is Monday..
    document.getElementById('day_of_week').value = dayForValue === 0 ? 7 : dayForValue;

    // Check model health on load
    fetch('/health')
        .then(res => res.json())
        .then(data => {
            if (data.status === 'healthy') {
                modelStatus.textContent = `Model: ${data.model_version}`;
                modelTag.textContent = `Production ${data.model_version}`;
            } else {
                modelStatus.textContent = 'Model: Offline';
                modelStatus.parentElement.querySelector('.dot').style.background = '#ef4444';
            }
        })
        .catch(() => {
            modelStatus.textContent = 'Model: Connection Error';
            modelStatus.parentElement.querySelector('.dot').style.background = '#ef4444';
        });

    // Chart.js init
    const ctx = document.getElementById('predictionChart').getContext('2d');
    let chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['Current', 'Forecast (24h)'],
            datasets: [{
                label: 'Temperature Profile',
                data: [null, null],
                borderColor: '#6366f1',
                backgroundColor: 'rgba(99, 102, 241, 0.1)',
                borderWidth: 3,
                tension: 0.4,
                fill: true,
                pointBackgroundColor: '#6366f1',
                pointRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    ticks: { color: '#94a3b8' }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#94a3b8' }
                }
            },
            plugins: {
                legend: { display: false }
            }
        }
    });

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        submitBtn.disabled = true;
        submitBtn.textContent = 'Analyzing Patterns...';
        
        const formData = new FormData(form);
        const requestData = {
            temperature: parseFloat(formData.get('temperature')),
            humidity: parseFloat(formData.get('humidity')),
            pressure: parseFloat(formData.get('pressure')),
            wind_speed: parseFloat(formData.get('wind_speed')),
            hour: parseInt(formData.get('hour')),
            month: parseInt(formData.get('month')),
            day_of_week: parseInt(formData.get('day_of_week'))
        };

        try {
            const response = await fetch('/predict', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestData)
            });

            if (!response.ok) throw new Error('Prediction Failed');

            const result = await response.json();
            
            // Update UI with results
            document.getElementById('pred-value').textContent = result.predicted_temperature_24h.toFixed(1);
            document.getElementById('conf-interval').textContent = 
                `${result.confidence_lower.toFixed(1)} to ${result.confidence_upper.toFixed(1)} °C`;
            document.getElementById('forecast-time').textContent = new Date(result.timestamp).toLocaleString();
            
            // Update Chart
            chart.data.datasets[0].data = [requestData.temperature, result.predicted_temperature_24h];
            chart.update();

            // Success feedback
            submitBtn.style.background = '#10b981';
            submitBtn.textContent = 'Forecast Ready';
            setTimeout(() => {
                submitBtn.style.background = '#6366f1';
                submitBtn.textContent = 'Generate Forecast';
                submitBtn.disabled = false;
            }, 2000);

        } catch (err) {
            console.error(err);
            alert('Error generating forecast. Please check if the model is loaded.');
            submitBtn.disabled = false;
            submitBtn.textContent = 'Generate Forecast';
        }
    });
});
